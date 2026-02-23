export interface SeededRng {
    next(): number;
    nextSigned(): number;
}

export function createSeededRng(seed = 1): SeededRng {
    let state = Math.floor(Number(seed)) >>> 0;
    if (state === 0) state = 1;

    const next = () => {
        // xorshift32
        state ^= state << 13;
        state ^= state >>> 17;
        state ^= state << 5;
        return (state >>> 0) / 4294967296;
    };

    return {
        next,
        nextSigned() {
            return next() * 2 - 1;
        },
    };
}
