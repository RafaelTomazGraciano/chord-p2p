'use strict';

const fs = require('node:fs/promises');
const path = require('node:path');
const { FINGER_COUNT, REPLICATION_FACTOR, add, hashKey, inInterval, validateId } = require('./ring');

const CATALOG_NAME = 'catalogo.txt';

class ChordNode {
  constructor({ id, host = '127.0.0.1', port = 5000, requestTimeout = 10000,
    storageDirectory } = {}) {
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

    this.successor = successor;
    this.predecessor = predecessor;

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
    return this.state();
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

    let current = this.successor;
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
      const next = response.node;
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
      finger.node = nodes[index];
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

    return this.rpc(next, '/rpc/find-successor', {
      method: 'POST',
      body: { id, hops: hops + 1 }
    });
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

  async rpc(node, path, { method = 'GET', body } = {}) {
    const target = normalizeReference(node);
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.requestTimeout);
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