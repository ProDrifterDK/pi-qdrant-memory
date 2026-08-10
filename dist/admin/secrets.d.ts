export declare const ADMIN_QDRANT_API_KEY_ENV: string;
export interface AdminProcessSecrets {
    destinationApiKey?: string;
}
export declare function loadAdminProcessSecrets(env: Record<string, string | undefined>): AdminProcessSecrets;
