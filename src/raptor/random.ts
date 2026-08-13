import { createHash } from "node:crypto";

const UINT32_RANGE = 0x1_0000_0000;
const NON_ZERO_FALLBACK = 0x9e3779b9;

function rotl(value: number, shift: number): number {
  return ((value << shift) | (value >>> (32 - shift))) >>> 0;
}

/** SHA-256(seed UTF-8) → the first 128 bits as four big-endian uint32 words. */
export function seedWords(seed: string | number): readonly [number, number, number, number] {
  if ((typeof seed !== "string" && typeof seed !== "number") || (typeof seed === "number" && !Number.isFinite(seed))) throw new TypeError("RAPTOR seed must be a bounded string or finite number");
  const text = String(seed);
  if (text.length === 0 || text.length > 4096) throw new TypeError("RAPTOR seed must be bounded");
  const digest = createHash("sha256").update(text, "utf8").digest();
  const words: [number, number, number, number] = [digest.readUInt32BE(0), digest.readUInt32BE(4), digest.readUInt32BE(8), digest.readUInt32BE(12)];
  if (words.every((word) => word === 0)) words[0] = NON_ZERO_FALLBACK;
  return Object.freeze(words);
}

/** Deterministic xoshiro128** PRNG with unbiased bounded integers. */
export class Xoshiro128StarStar {
  readonly #state: [number, number, number, number];
  constructor(seed: string | number) { this.#state = [...seedWords(seed)] as [number, number, number, number]; }
  nextUint32(): number {
    const state = this.#state;
    const result = Math.imul(rotl(Math.imul(state[1], 5) >>> 0, 7), 9) >>> 0;
    const shifted = (state[1] << 9) >>> 0;
    state[2] = (state[2] ^ state[0]) >>> 0;
    state[3] = (state[3] ^ state[1]) >>> 0;
    state[1] = (state[1] ^ state[2]) >>> 0;
    state[0] = (state[0] ^ state[3]) >>> 0;
    state[2] = (state[2] ^ shifted) >>> 0;
    state[3] = rotl(state[3], 11);
    return result;
  }
  nextFloat(): number { return this.nextUint32() / UINT32_RANGE; }
  nextInt(maxExclusive: number): number {
    if (!Number.isSafeInteger(maxExclusive) || maxExclusive < 1 || maxExclusive > UINT32_RANGE) throw new TypeError("PRNG maxExclusive must be in 1..2^32");
    const limit = Math.floor(UINT32_RANGE / maxExclusive) * maxExclusive;
    let sample: number;
    do { sample = this.nextUint32(); } while (sample >= limit);
    return sample % maxExclusive;
  }
  random = (): number => this.nextFloat();
}
Object.freeze(Xoshiro128StarStar.prototype);
