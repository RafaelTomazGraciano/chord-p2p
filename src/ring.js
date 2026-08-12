'use strict';

const crypto = require('node:crypto');

const RING_SIZE = 32;
const FINGER_COUNT = 5;
// Número de sucessores (r) usados para replicar cada arquivo (seção 5.2 do artigo).
// É independente de FINGER_COUNT (m): a finger table serve para roteamento,
// a successor-list é quem define onde as réplicas ficam.
const REPLICATION_FACTOR = 5;

function validateId(id) {
  const value = Number(id);
  if (!Number.isInteger(value) || value < 1 || value > RING_SIZE) {
    throw new Error(`O id deve ser um inteiro entre 1 e ${RING_SIZE}`);
  }
  return value;
}

// Converte a representação pública 1..32 para a representação modular 0..31.
function toRing(id) {
  return validateId(id) % RING_SIZE;
}

function fromRing(value) {
  const normalized = ((value % RING_SIZE) + RING_SIZE) % RING_SIZE;
  return normalized === 0 ? RING_SIZE : normalized;
}

function add(id, offset) {
  return fromRing(toRing(id) + offset);
}

// Mapeia uma chave para uma das 32 posições do anel público (1..32).
function hashKey(key) {
  if (typeof key !== 'string' || key.length === 0) {
    throw new Error('A chave deve ser uma string não vazia');
  }
  const digest = crypto.createHash('sha256').update(key, 'utf8').digest();
  return (digest.readUInt32BE(0) % RING_SIZE) + 1;
}

// Intervalo circular. Ex.: (30, 3] contém 31, 32, 1, 2 e 3.
function inInterval(id, start, end, includeStart = false, includeEnd = false) {
  const point = toRing(id);
  const left = toRing(start);
  const right = toRing(end);

  if (left === right) {
    if (!includeStart && !includeEnd) return point !== left;
    return true;
  }

  if (left < right) {
    return (point > left || (includeStart && point === left))
      && (point < right || (includeEnd && point === right));
  }

  return point > left || point < right
    || (includeStart && point === left)
    || (includeEnd && point === right);
}

module.exports = {
  RING_SIZE,
  FINGER_COUNT,
  REPLICATION_FACTOR,
  validateId,
  add,
  inInterval,
  hashKey
};