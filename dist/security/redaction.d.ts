import type { RedactionStatus, SecretScanStatus } from "../types.js";
/** Output of structural redaction before the final scanner runs. */
export interface StructuralRedactionResult {
    readonly text: string;
    readonly redactionStatus: RedactionStatus;
    readonly contentHash: string;
}
/** Final, egress-eligible material. Structural output is deliberately not this type. */
export interface RedactionResult extends StructuralRedactionResult {
    readonly secretScan: SecretScanStatus;
    readonly dropped: boolean;
}
export type SecretScanner = (text: string) => SecretScanStatus;
export interface RedactAndScanInput {
    readonly text: string;
    readonly maxChars: number;
    readonly homeDir: string;
    readonly scan?: SecretScanner;
}
export declare function redactStructure(input: {
    text: string;
    maxChars: number;
    homeDir: string;
}): StructuralRedactionResult;
export declare function redactAndScan(input: RedactAndScanInput): RedactionResult;
export declare function redactField(input: {
    name: string;
    value: string;
    maxChars: number;
    homeDir: string;
}): RedactionResult;
