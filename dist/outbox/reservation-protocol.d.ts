export interface ReservationProtocolFileSystem {
    chmod(path: string, mode: number): Promise<void>;
    link(existingPath: string, newPath: string): Promise<void>;
    lstat(path: string): Promise<{
        isDirectory(): boolean;
        isFile(): boolean;
        isSymbolicLink(): boolean;
        mode: number;
        size: number;
        dev: number;
        ino: number;
    }>;
    open(path: string, flags: string | number, mode?: number): Promise<{
        writeFile(data: string, encoding?: BufferEncoding): Promise<void>;
        readFile(): Promise<Uint8Array>;
        stat(): Promise<{
            isDirectory(): boolean;
            isFile(): boolean;
            isSymbolicLink(): boolean;
            mode: number;
            size: number;
            dev: number;
            ino: number;
        }>;
        sync(): Promise<void>;
        close(): Promise<void>;
    }>;
    readFile(path: string, encoding: BufferEncoding): Promise<string>;
    readdir(path: string): Promise<string[]>;
    rm(path: string, options?: {
        force?: boolean;
        recursive?: boolean;
    }): Promise<void>;
}
export interface ReservationRecord {
    version: 1;
    reservationId: string;
    jobId: string;
    jobAuditHash: string;
    policyId: string;
    deadline: string | null;
    nodeId: string;
    producerUuid: string;
    requestedBytes: number;
    auditHash: string;
}
export declare const ADMISSION_GENERATION_LIMIT = 1000000;
export declare const ADMISSION_LOCK: RegExp;
export declare const ADMISSION_RETIREMENT: RegExp;
export declare function admissionLockName(generation: number): string;
export declare function admissionRetirementName(generation: number): string;
export declare function isAdmissionProtocolArtifact(name: string): boolean;
export declare function publishAdmissionRetirement<T extends ReservationRecord>(fs: ReservationProtocolFileSystem, dir: string, generation: number, reservation: T, validateReservation: (value: unknown) => T): Promise<void>;
export declare function activeAdmissionLocks<T extends ReservationRecord>(fs: ReservationProtocolFileSystem, dir: string, validateReservation: (value: unknown) => T): Promise<Array<{
    generation: number;
    file: string;
    reservation: T;
}>>;
export declare function acquireAdmissionGeneration<T extends ReservationRecord>(input: {
    fs: ReservationProtocolFileSystem;
    dir: string;
    reservationFile: string;
    reservation: T;
    validateReservation: (value: unknown) => T;
    durableProof: (reservation: T) => Promise<boolean>;
    abandoned?: (reservation: T) => Promise<boolean>;
    busyDelayMs?: number;
    maxAttempts?: number;
    busyDeadlineMs?: number;
    now?: () => number;
}): Promise<{
    generation: number;
    file: string;
    reservation: T;
}>;
export declare function retireOwnedAdmissionLock<T extends ReservationRecord>(input: {
    fs: ReservationProtocolFileSystem;
    dir: string;
    reservation: T;
    validateReservation: (value: unknown) => T;
    requireOwnership?: boolean;
}): Promise<void>;
