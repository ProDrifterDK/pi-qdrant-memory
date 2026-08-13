/** SHA-256(seed UTF-8) → the first 128 bits as four big-endian uint32 words. */
export declare function seedWords(seed: string | number): readonly [number, number, number, number];
/** Deterministic xoshiro128** PRNG with unbiased bounded integers. */
export declare class Xoshiro128StarStar {
    #private;
    constructor(seed: string | number);
    nextUint32(): number;
    nextFloat(): number;
    nextInt(maxExclusive: number): number;
    random: () => number;
}
