'use strict';

const fs = require('node:fs/promises');
const path = require('node:path');
const { FINGER_COUNT, REPLICATION_FACTOR, add, hashKey, inInterval, validateId } = require('./ring');

const CATALOG_NAME = 'catalogo.txt';

class ChordNode {
  constructor({ id, host = '127.0.0.1', port = 5000, requestTimeout = 10000,
    probeTimeout = 1500, stabilizeInterval = 2000, fixFingersInterval = 3000,
    checkPredecessorInterval = 2500, storageDirectory } = {}) {
    this.id = validateId(id);
    this.host = String(host || '').trim();
    if (!this.host || this.host === '0.0.0.0' || this.host === '::') {
      throw new Error('Informe o IP ou hostname pelo qual os outros nós acessam esta máquina');
    }
    this.port = Number(port);
    if (!Number.isInteger(this.port) || this.port < 1 || this.port > 65535) {
      throw new Error('A porta deve ser um inteiro entre 1 e 65535');
    }
    this.requestTimeout = requestTimeout;
    // Timeout curto para as sondagens periódicas (stabilize, check-predecessor):
    // não faz sentido esperar 10s por um nó que provavelmente caiu.
    this.probeTimeout = probeTimeout;
    this.stabilizeInterval = stabilizeInterval;
    this.fixFingersInterval = fixFingersInterval;
    this.checkPredecessorInterval = checkPredecessorInterval;
    this.storageDirectory = storageDirectory || path.join(
      process.cwd(), 'data', `node-${this.id}-${this.port}`);
    // Arquivos dos quais este nó é o dono (owner) ficam em primary/.
    // Cópias que este nó guarda a pedido de outro dono ficam em replica/<ownerId>/.
    this.primaryDirectory = path.join(this.storageDirectory, 'primary');
    this.replicaDirectory = path.join(this.storageDirectory, 'replica');
    this.predecessor = null;
    this.fingers = this.buildEmptyFingerTable();
    // Os r nós que sucedem este nó no anel (seção 5.2 do artigo). É quem
    // recebe réplica dos arquivos deste nó; é diferente da finger table,
    // que serve para roteamento e pula distâncias maiores.
    this.successorList = [];
    this.joined = false;
    this._timers = [];
    this._maintenanceStarted = false;
    this._stabilizing = false;
  }

  get reference() {
    return { id: this.id, host: this.host, port: this.port };
  }

  buildEmptyFingerTable() {
    return Array.from({ length: FINGER_COUNT }, (_, index) => ({
      index: index + 1,
      start: add(this.id, 2 ** index),
      node: null
    }));
  }

  get successor() {
    return this.fingers[0].node;
  }

  set successor(node) {
    this.fingers[0].node = node;
  }

  createRing() {
    this.predecessor = this.reference;
    for (const finger of this.fingers) finger.node = this.reference;
    this.successorList = []; // sozinho no anel, não há para quem replicar ainda
    this.joined = true;
    this.startMaintenance();
  }

  async join(bootstrap) {
    if (this.joined) throw new Error('Este nó já pertence a uma rede Chord');
    if (!bootstrap) {
      this.createRing();
      return this.state();
    }

    const contact = normalizeReference(bootstrap);
    if (contact.id === this.id) throw new Error('O nó de entrada não pode ter o mesmo id');

    // Localiza a posição do novo nó no anel usando o nó de entrada.
    const successor = await this.rpc(contact, '/rpc/find-successor', {
      method: 'POST',
      body: { id: this.id }
    });
    if (successor.id === this.id) throw new Error(`O id ${this.id} já está em uso`);

    const predecessorResult = await this.rpc(successor, '/rpc/predecessor');
    const predecessor = predecessorResult.node || successor;

    // Guarda apenas id/host/port. A resposta de find-successor vem
    // decorada com "replicas" (dica para leitura via réplica); se isso
    // fosse gravado direto em this.successor, cada ciclo de stabilize
    // reembrulharia o valor anterior dentro de um novo "replicas",
    // crescendo sem limite até o /api/state não conseguir mais ser
    // serializado (era a causa do ERR_HTTP_HEADERS_SENT no painel).
    this.successor = normalizeReference(successor);
    this.predecessor = normalizeReference(predecessor);

    // Pede ao sucessor os arquivos que passam a pertencer a este nó (aqueles
    // cujo hash cai no intervalo (predecessor, this.id]). Precisa acontecer
    // antes de atualizar os ponteiros do anel, enquanto o sucessor ainda é
    // formalmente o dono desses arquivos.
    await this.takeOverKeys(predecessor.id, successor);

    // Faz o novo nó entrar entre predecessor e sucessor.
    await this.rpc(successor, '/rpc/predecessor', {
      method: 'PUT',
      body: { node: this.reference }
    });
    if (predecessor.id !== successor.id) {
      await this.rpc(predecessor, '/rpc/successor', {
        method: 'PUT',
        body: { node: this.reference }
      });
    } else {
      // A rede possuía apenas um nó.
      await this.rpc(successor, '/rpc/successor', {
        method: 'PUT',
        body: { node: this.reference }
      });
    }

    this.joined = true;

    await this.refreshFingerTable();
    const previousSuccessorList = this.successorList;
    await this.refreshSuccessorList();
    // Nó recém-chegado normalmente não tem arquivos próprios ainda, mas
    // rodamos mesmo assim por consistência com o restante do fluxo.
    await this.syncReplicationTargets(previousSuccessorList);

    // A entrada altera também as fingers dos nós que já estavam no anel.
    await this.rpc(this.successor, '/rpc/refresh-fingers', {
      method: 'POST',
      body: { originId: this.id, hops: 0 }
    });
    this.startMaintenance();
    return this.state();
  }

  /**
   * Inicia as três rotinas periódicas do Chord clássico. Sem elas, os
   * ponteiros de predecessor/sucessor e a finger table só são calculados
   * uma vez, no join, e nunca mais se corrigem sozinhos: quando um nó sai
   * da rede sem avisar (o caso comum aqui, já que não há um "leave"), os
   * vizinhos continuam apontando para um nó morto para sempre e o anel
   * trava. stabilize + notify corrigem sucessor/predecessor, fixFingers
   * corrige o roteamento e checkPredecessor detecta a saída de quem
   * apontava para este nó.
   */
  startMaintenance() {
    if (this._maintenanceStarted) return;
    this._maintenanceStarted = true;
    this._timers.push(setInterval(() => {
      this.stabilize().catch((error) =>
        console.error(`stabilize (nó ${this.id}): ${error.message}`));
    }, this.stabilizeInterval));
    this._timers.push(setInterval(() => {
      this.fixFingers().catch((error) =>
        console.error(`fix-fingers (nó ${this.id}): ${error.message}`));
    }, this.fixFingersInterval));
    this._timers.push(setInterval(() => {
      this.checkPredecessor().catch((error) =>
        console.error(`check-predecessor (nó ${this.id}): ${error.message}`));
    }, this.checkPredecessorInterval));
  }

  stopMaintenance() {
    for (const timer of this._timers) clearInterval(timer);
    this._timers = [];
    this._maintenanceStarted = false;
  }

  /**
   * Verifica se o sucessor ainda é o nó correto e avisa esse sucessor de
   * que este nó existe (notify), para que ele possa atualizar o próprio
   * predecessor. Roda periodicamente; é o coração da auto-recuperação do
   * anel.
   */
  async stabilize() {
    if (!this.joined || this._stabilizing) return;
    this._stabilizing = true;
    try {
      if (!this.successor || this.successor.id !== this.id) {
        let predecessorOfSuccessor = null;
        if (this.successor) {
          try {
            const result = await this.rpc(this.successor, '/rpc/predecessor',
              { timeout: this.probeTimeout });
            predecessorOfSuccessor = result.node;
          } catch (error) {
            // O sucessor não respondeu: provavelmente saiu da rede sem
            // avisar. Usa a successor-list (mantida para replicação) para
            // achar o próximo nó vivo, em vez de deixar o anel travado.
            await this.repairSuccessor();
          }
        } else {
          await this.repairSuccessor();
        }

        if (this.successor && this.successor.id !== this.id
          && predecessorOfSuccessor && predecessorOfSuccessor.id !== this.id
          && inInterval(predecessorOfSuccessor.id, this.id, this.successor.id, false, false)) {
          this.successor = normalizeReference(predecessorOfSuccessor);
        }
      }

      if (this.successor && this.successor.id !== this.id) {
        try {
          await this.rpc(this.successor, '/rpc/notify', {
            method: 'POST',
            body: { node: this.reference },
            timeout: this.probeTimeout
          });
        } catch (error) {
          // Tenta de novo no próximo ciclo.
        }
      }

      const previousSuccessorList = this.successorList;
      await this.refreshSuccessorList();
      await this.syncReplicationTargets(previousSuccessorList);
    } finally {
      this._stabilizing = false;
    }
  }

  /**
   * Substitui um sucessor que parou de responder pelo próximo nó vivo na
   * successor-list atual (os até REPLICATION_FACTOR nós seguintes no
   * anel). Se nenhum responder, este nó ficou isolado temporariamente e
   * assume a si mesmo como sucessor, até que outro nó o encontre de novo
   * pelas próprias finger tables e o notifique.
   */
  async repairSuccessor() {
    for (const candidate of this.successorList) {
      if (candidate.id === this.id) continue;
      try {
        await this.rpc(candidate, '/rpc/predecessor', { timeout: this.probeTimeout });
        this.successor = normalizeReference(candidate);
        return;
      } catch (error) {
        continue; // tenta o próximo da successor-list
      }
    }
    this.successor = this.reference;
  }

  /**
   * Chamado por outro nó (via /rpc/notify) para se anunciar como possível
   * novo predecessor. Só aceita se o predecessor atual está vazio, morto
   * (será confirmado por checkPredecessor) ou se o candidato está de fato
   * mais perto deste nó no anel.
   */
  notify(candidate) {
    const node = normalizeReference(candidate);
    if (node.id === this.id) return;
    if (!this.predecessor || this.predecessor.id === this.id
      || inInterval(node.id, this.predecessor.id, this.id, false, false)) {
      this.predecessor = node;
    }
  }

  /**
   * Reconsulta cada entrada da finger table periodicamente, para que o
   * roteamento se corrija sozinho quando um nó referenciado por uma
   * finger sai da rede.
   */
  async fixFingers() {
    if (!this.joined) return;
    await this.refreshFingerTable();
  }

  /**
   * Sonda o predecessor periodicamente. Se ele não responder, limpa o
   * ponteiro (o próximo notify de outro nó o substitui) e promove as
   * réplicas que este nó guardava para ele, já que este nó passa a ser o
   * dono responsável por aquela faixa de ids.
   */
  async checkPredecessor() {
    if (!this.joined || !this.predecessor || this.predecessor.id === this.id) return;
    const failedId = this.predecessor.id;
    try {
      await this.rpc(this.predecessor, '/rpc/predecessor', { timeout: this.probeTimeout });
    } catch (error) {
      this.predecessor = null;
      await this.promoteReplica(failedId).catch((promotionError) => {
        console.error(`Não foi possível promover réplicas do nó ${failedId} no nó ${this.id}: ${promotionError.message}`);
      });
    }
  }

  /**
   * Move para primary/ os arquivos que este nó guardava como réplica de
   * um dono (ownerId) que saiu da rede sem avisar. Este nó só chama isso
   * quando é ele mesmo quem detecta a queda do predecessor, ou seja,
   * exatamente o nó que passa a ser responsável por aquela faixa de ids.
   * Arquivos que já existirem em primary/ com o mesmo nome não são
   * sobrescritos.
   */
  async promoteReplica(ownerId) {
    const ownerDirectory = path.join(this.replicaDirectory, String(ownerId));
    let files;
    try {
      files = await fs.readdir(ownerDirectory);
    } catch (error) {
      if (error.code === 'ENOENT') return;
      throw error;
    }
    await fs.mkdir(this.primaryDirectory, { recursive: true });
    for (const file of files) {
      const from = path.join(ownerDirectory, file);
      const to = path.join(this.primaryDirectory, file);
      try {
        await fs.access(to);
        continue; // já existe um arquivo com esse nome; a cópia local prevalece
      } catch {
        await fs.rename(from, to);
      }
    }
    await fs.rm(ownerDirectory, { recursive: true, force: true }).catch(() => {});
  }

  /**
   * Saída graciosa da rede, ao contrário de simplesmente derrubar o
   * processo. Entrega ao sucessor os arquivos dos quais este nó era dono
   * (ele passa a ser o novo dono, exatamente como aconteceria depois de
   * uma queda detectada por checkPredecessor, só que sem esperar nenhum
   * ciclo de estabilização) e liga predecessor e sucessor um ao outro
   * diretamente, pulando este nó. Depois disso o nó volta ao estado
   * "fora do anel" e pode entrar de novo (join) se quiser.
   */
  async leave() {
    this.assertJoined();
    this.stopMaintenance();

    if (this.successor && this.successor.id !== this.id) {
      const files = await this.listPrimaryFiles();
      for (const name of files) {
        try {
          const content = await this.readLocal(name);
          await this.rpc(this.successor, '/rpc/files', {
            method: 'PUT',
            body: { name, content: content.toString('base64') }
          });
        } catch (error) {
          console.error(`Não foi possível entregar "${name}" ao sucessor ao sair: ${error.message}`);
        }
      }

      if (this.predecessor && this.predecessor.id !== this.id) {
        await this.rpc(this.successor, '/rpc/predecessor', {
          method: 'PUT', body: { node: this.predecessor }
        }).catch(() => {});
        await this.rpc(this.predecessor, '/rpc/successor', {
          method: 'PUT', body: { node: this.successor }
        }).catch(() => {});
        // Reforça via notify, para o stabilize do sucessor confirmar o novo predecessor.
        await this.rpc(this.successor, '/rpc/notify', {
          method: 'POST', body: { node: this.predecessor }
        }).catch(() => {});
      }
    }

    this.joined = false;
    this.predecessor = null;
    this.fingers = this.buildEmptyFingerTable();
    this.successorList = [];
    return this.state();
  }

  /**
   * Chamado pelo nó recém-chegado logo depois de descobrir seu predecessor
   * e sucessor. Sem isso, a posse de um arquivo muda de dono no roteamento
   * assim que alguém entra no anel (porque findSuccessor passa a apontar
   * para o novo nó), mas os bytes continuam só no disco do dono antigo.
   * É exatamente por isso que o catálogo parecia "resetar": o novo dono
   * não tinha nenhuma cópia local e recomeçava um catálogo vazio.
   */
  async takeOverKeys(predecessorId, successorNode) {
    if (successorNode.id === this.id) return; // sozinho no anel, nada a transferir
    let response;
    try {
      response = await this.rpc(successorNode, '/rpc/transfer-keys', {
        method: 'POST',
        body: { newNodeId: this.id, predecessorId }
      });
    } catch (error) {
      console.error(`Não foi possível transferir arquivos do nó ${successorNode.id}: ${error.message}`);
      return;
    }
    for (const file of response.files || []) {
      const content = Buffer.from(file.content, 'base64');
      await this.storeLocal(file.name, content);
    }
  }

  /**
   * Atende ao pedido de um nó que acabou de entrar entre `predecessorId` e
   * `newNodeId`: devolve e remove localmente os arquivos deste nó cujo
   * hash caiu nesse intervalo, já que a posse deles muda de dono.
   */
  async transferKeys(newNodeId, predecessorId) {
    const files = await this.listPrimaryFiles();
    const transferred = [];
    for (const name of files) {
      const hashId = hashKey(name);
      if (inInterval(hashId, predecessorId, newNodeId, false, true)) {
        const content = await this.readLocal(name);
        transferred.push({ name, content: content.toString('base64') });
      }
    }

    for (const file of transferred) {
      await fs.unlink(path.join(this.primaryDirectory, file.name)).catch(() => {});
    }

    // As réplicas antigas desses arquivos, guardadas pela successor-list
    // deste nó, ficaram órfãs: o novo dono monta as próprias ao longo da
    // sua própria successor-list. Apaga aqui para não deixar cópias
    // divergentes espalhadas pela rede.
    if (transferred.length > 0) {
      await Promise.all(this.successorList.map((node) =>
        Promise.all(transferred.map((file) =>
          this.rpc(node, '/rpc/replicate', {
            method: 'DELETE',
            body: { ownerId: this.id, name: file.name }
          }).catch(() => {})))));
    }

    return { files: transferred };
  }

  /**
   * Recalcula a successor-list (os até REPLICATION_FACTOR nós que sucedem
   * este nó no anel), andando pelos ponteiros de sucessor a partir do
   * sucessor imediato. Segue a seção 5.2 do artigo: essa lista é o que
   * define onde as réplicas de cada arquivo deste nó ficam guardadas.
   */
  async refreshSuccessorList() {
    const list = [];
    if (!this.successor) {
      this.successorList = list;
      return list;
    }

    let current = normalizeReference(this.successor);
    list.push(current);

    while (list.length < REPLICATION_FACTOR && current.id !== this.id) {
      let response;
      try {
        response = await this.rpc(current, '/rpc/successor');
      } catch (error) {
        // Nó inacessível: para por aqui. A lista fica mais curta até a
        // próxima estabilização, o que é aceitável (comportamento
        // degradado, não incorreto).
        break;
      }
      const next = response.node && normalizeReference(response.node);
      if (!next || next.id === this.id) break; // completou a volta no anel
      list.push(next);
      current = next;
    }

    this.successorList = list;
    return list;
  }

  /**
   * Compara a successor-list antiga com a atual e mantém as réplicas dos
   * arquivos deste nó sincronizadas: empurra réplica para quem entrou na
   * lista e apaga a réplica de quem saiu.
   */
  async syncReplicationTargets(previousList = []) {
    const previousIds = new Set(previousList.map((node) => node.id));
    const currentIds = new Set(this.successorList.map((node) => node.id));
    const added = this.successorList.filter((node) => !previousIds.has(node.id));
    const removed = previousList.filter((node) => !currentIds.has(node.id));
    if (added.length === 0 && removed.length === 0) return;

    const files = await this.listPrimaryFiles();
    if (files.length === 0) return;

    for (const file of files) {
      const content = await this.readLocal(file);
      await Promise.all(added.map((node) =>
        this.rpc(node, '/rpc/replicate', {
          method: 'PUT',
          body: { ownerId: this.id, name: file, content: content.toString('base64') }
        }).catch((error) => {
          console.error(`Não foi possível replicar "${file}" no nó ${node.id}: ${error.message}`);
        })));
    }

    for (const node of removed) {
      await Promise.all(files.map((file) =>
        this.rpc(node, '/rpc/replicate', {
          method: 'DELETE',
          body: { ownerId: this.id, name: file }
        }).catch(() => {
          // Limpeza é best-effort; se o nó que saiu da lista já caiu ou
          // está inacessível, não há problema em deixar a réplica órfã lá.
        })));
    }
  }

  /** Envia uma cópia do arquivo para todos os nós da successor-list atual. */
  async replicateToSuccessors(fileName, content) {
    const name = validateFileName(fileName);
    const bytes = Buffer.isBuffer(content) ? content : Buffer.from(content);
    await Promise.all(this.successorList.map((node) =>
      this.rpc(node, '/rpc/replicate', {
        method: 'PUT',
        body: { ownerId: this.id, name, content: bytes.toString('base64') }
      }).catch((error) => {
        console.error(`Não foi possível replicar "${name}" no nó ${node.id}: ${error.message}`);
      })));
  }

  async refreshFingerTable() {
    const nodes = await Promise.all(this.fingers.map((finger) =>
      this.findSuccessor(finger.start)));
    this.fingers.forEach((finger, index) => {
      // this.findSuccessor devolve o nó decorado com "replicas" (dica de
      // leitura); guardar isso na finger table faria o campo crescer a
      // cada ciclo de fixFingers, então só o essencial (id/host/port) fica.
      finger.node = normalizeReference(nodes[index]);
    });
  }

  async refreshRingFingerTables(originId, hops = 0) {
    validateId(originId);
    if (this.id === Number(originId)) return { ok: true };
    if (hops >= 32) throw new Error('Limite de nós excedido ao atualizar finger tables');

    await this.refreshFingerTable();
    const previousSuccessorList = this.successorList;
    await this.refreshSuccessorList();
    await this.syncReplicationTargets(previousSuccessorList);

    // Cada nó responde após atualizar a própria tabela. O próximo salto ocorre
    // fora da requisição atual para o tempo total não crescer com o anel.
    const next = this.successor;
    setImmediate(() => {
      this.rpc(next, '/rpc/refresh-fingers', {
        method: 'POST',
        body: { originId: Number(originId), hops: hops + 1 }
      }).catch((error) => {
        console.error(`Não foi possível atualizar as fingers após o nó ${this.id}: ${error.message}`);
      });
    });
    return { ok: true };
  }

  async findSuccessor(rawId, hops = 0) {
    const id = validateId(rawId);
    if (!this.joined || !this.successor) throw new Error('O nó ainda não entrou em uma rede');
    if (this.successor.id === this.id) return this.withReplicaHints(this.reference);
    if (id === this.id) return this.withReplicaHints(this.reference);

    if (inInterval(id, this.id, this.successor.id, false, true)) {
      return this.withReplicaHints(this.successor);
    }

    if (hops >= 32) throw new Error('Limite de saltos excedido ao procurar sucessor');
    let next = this.closestPrecedingFinger(id);
    // Uma finger table ainda desatualizada não deve interromper a busca:
    // caminhar pelo sucessor sempre encontra a posição correta no anel.
    if (next.id === this.id) next = this.successor;

    try {
      return await this.rpc(next, '/rpc/find-successor', {
        method: 'POST',
        body: { id, hops: hops + 1 }
      });
    } catch (error) {
      if (next.id === this.successor.id) throw error; // já era o último recurso
      // A finger usada apontava para um nó fora do ar (fixFingers ainda não
      // teve tempo de corrigir essa entrada). Tenta pelo sucessor direto
      // antes de desistir, em vez de derrubar a busca inteira.
      return this.rpc(this.successor, '/rpc/find-successor', {
        method: 'POST',
        body: { id, hops: hops + 1 }
      });
    }
  }

  closestPrecedingFinger(id) {
    for (let i = this.fingers.length - 1; i >= 0; i -= 1) {
      const candidate = this.fingers[i].node;
      if (candidate && candidate.id !== this.id
        && inInterval(candidate.id, this.id, id, false, false)) {
        return candidate;
      }
    }
    return this.reference;
  }

  /**
   * Anexa a uma referência de nó os candidatos a réplica conhecidos por
   * este nó. Quando este nó é quem responde (owner === this), sua própria
   * successor-list é exatamente onde as réplicas estão. Quando quem
   * responde é o predecessor do owner (owner === this.successor), a
   * successor-list do predecessor já começa com [owner, sucessores de
   * owner...], então os candidatos são essa lista sem o próprio owner —
   * uma aproximação correta em estado estável, e apenas potencialmente
   * desatualizada sob concorrência, como o resto do roteamento do Chord.
   */
  withReplicaHints(node) {
    return { ...node, replicas: this.successorList.filter((candidate) => candidate.id !== node.id) };
  }

  /** Insere bytes na rede e devolve a posição do hash e o nó responsável. */
  async put(fileName, content, { updateCatalog = true } = {}) {
    this.assertJoined();
    const name = validateFileName(fileName);
    const bytes = Buffer.isBuffer(content) ? content : Buffer.from(content);
    const hashId = hashKey(name);
    const owner = await this.findSuccessor(hashId);

    if (owner.id === this.id) {
      await this.storeLocal(name, bytes);
      await this.replicateToSuccessors(name, bytes);
    } else {
      await this.rpc(owner, '/rpc/files', {
        method: 'PUT',
        body: { name, content: bytes.toString('base64') }
      });
    }

    if (updateCatalog && name !== CATALOG_NAME) await this.addToCatalog(name);
    return { name, hashId, node: owner, size: bytes.length };
  }

  /**
   * Busca os bytes de um arquivo a partir de qualquer nó da rede. Se o dono
   * não responder, tenta os nós da successor-list dele (onde as réplicas
   * ficam guardadas), na ordem em que aparecem — seção 5.2 do artigo.
   */
  async get(fileName) {
    this.assertJoined();
    const name = validateFileName(fileName);
    const hashId = hashKey(name);
    const owner = await this.findSuccessor(hashId);
    let content;

    if (owner.id === this.id) {
      content = await this.readLocal(name);
    } else {
      try {
        const result = await this.rpc(owner, `/rpc/files?name=${encodeURIComponent(name)}`);
        content = Buffer.from(result.content, 'base64');
      } catch (error) {
        content = await this.readFromReplicas(owner, name, error);
      }
    }
    return { name, hashId, node: owner, size: content.length, content };
  }

  /** Tenta ler uma réplica do arquivo nos candidatos indicados pelo roteamento. */
  async readFromReplicas(owner, fileName, originalError) {
    const name = validateFileName(fileName);
    for (const candidate of owner.replicas || []) {
      try {
        const result = await this.rpc(
          candidate, `/rpc/replica?ownerId=${owner.id}&name=${encodeURIComponent(name)}`);
        return Buffer.from(result.content, 'base64');
      } catch (error) {
        continue; // tenta o próximo candidato
      }
    }
    // Nenhuma réplica disponível: propaga a falha original de acesso ao dono.
    throw originalError;
  }

  async addToCatalog(fileName) {
    let names = [];
    try {
      const catalog = await this.get(CATALOG_NAME);
      names = catalog.content.toString('utf8').split(/\r?\n/).filter(Boolean);
    } catch (error) {
      if (error.code !== 'ENOENT' && !/não encontrado/i.test(error.message)) throw error;
    }
    if (!names.includes(fileName)) names.push(fileName);
    names.sort((a, b) => a.localeCompare(b, 'pt-BR'));
    await this.put(CATALOG_NAME, Buffer.from(`${names.join('\n')}\n`), {
      updateCatalog: false
    });
  }

  async storeLocal(fileName, content) {
    const name = validateFileName(fileName);
    await fs.mkdir(this.primaryDirectory, { recursive: true });
    await fs.writeFile(path.join(this.primaryDirectory, name), content);
  }

  async readLocal(fileName) {
    const name = validateFileName(fileName);
    try {
      return await fs.readFile(path.join(this.primaryDirectory, name));
    } catch (error) {
      if (error.code === 'ENOENT') {
        const notFound = new Error(`Arquivo "${name}" não encontrado na rede`);
        notFound.code = 'ENOENT';
        throw notFound;
      }
      throw error;
    }
  }

  /** Lista os nomes dos arquivos dos quais este nó é o dono (primary). */
  async listPrimaryFiles() {
    try {
      return await fs.readdir(this.primaryDirectory);
    } catch (error) {
      if (error.code === 'ENOENT') return [];
      throw error;
    }
  }

  /** Grava a cópia (réplica) de um arquivo cujo dono é outro nó. */
  async storeReplica(ownerId, fileName, content) {
    const name = validateFileName(fileName);
    const ownerDirectory = path.join(this.replicaDirectory, String(validateId(ownerId)));
    await fs.mkdir(ownerDirectory, { recursive: true });
    await fs.writeFile(path.join(ownerDirectory, name), content);
  }

  /** Lê a réplica de um arquivo pertencente a outro nó (ownerId). */
  async readReplica(ownerId, fileName) {
    const name = validateFileName(fileName);
    try {
      return await fs.readFile(
        path.join(this.replicaDirectory, String(validateId(ownerId)), name));
    } catch (error) {
      if (error.code === 'ENOENT') {
        const notFound = new Error(`Réplica de "${name}" não encontrada neste nó`);
        notFound.code = 'ENOENT';
        throw notFound;
      }
      throw error;
    }
  }

  /** Remove a réplica de um arquivo; usado quando este nó sai da successor-list do dono. */
  async deleteReplica(ownerId, fileName) {
    const name = validateFileName(fileName);
    try {
      await fs.unlink(path.join(this.replicaDirectory, String(validateId(ownerId)), name));
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
  }

  assertJoined() {
    if (!this.joined) throw new Error('O nó ainda não entrou em uma rede');
  }

  async rpc(node, path, { method = 'GET', body, timeout } = {}) {
    const target = normalizeReference(node);
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeout || this.requestTimeout);
    try {
      const response = await fetch(`http://${target.host}:${target.port}${path}`, {
        method,
        headers: body ? { 'content-type': 'application/json' } : undefined,
        body: body ? JSON.stringify(body) : undefined,
        signal: controller.signal
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.error || `Erro HTTP ${response.status}`);
      return data;
    } catch (error) {
      if (error.name === 'AbortError') {
        const timeout = new Error(
          `Tempo limite ao acessar o nó ${target.id} em ${target.host}:${target.port}`);
        timeout.code = 'ETIMEDOUT';
        throw timeout;
      }
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }

  state() {
    return {
      node: this.reference,
      joined: this.joined,
      predecessor: this.predecessor,
      successor: this.successor,
      fingerTable: this.fingers,
      successorList: this.successorList
    };
  }
}

function validateFileName(fileName) {
  if (typeof fileName !== 'string' || !fileName.trim()) {
    throw new Error('O nome do arquivo é obrigatório');
  }
  const name = fileName.trim();
  if (name === '.' || name === '..' || path.basename(name) !== name
    || name.includes('/') || name.includes('\\') || name.includes('\0')) {
    throw new Error('Nome de arquivo inválido');
  }
  return name;
}

function normalizeReference(node) {
  if (!node || typeof node !== 'object') throw new Error('Referência de nó inválida');
  return {
    id: validateId(node.id),
    host: String(node.host || '127.0.0.1'),
    port: Number(node.port || 5000)
  };
}

module.exports = { ChordNode, normalizeReference, validateFileName, CATALOG_NAME };