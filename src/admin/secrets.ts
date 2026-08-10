const PREFIX = "PI_QDRANT_MEMORY_";
const ADMIN_SUFFIX = ["ADMIN", "_QDRANT_API_KEY"].join("");
export const ADMIN_QDRANT_API_KEY_ENV = `${PREFIX}${ADMIN_SUFFIX}`;

export interface AdminProcessSecrets {
  destinationApiKey?: string;
}

export function loadAdminProcessSecrets(env: Record<string, string | undefined>): AdminProcessSecrets {
  const value = env[ADMIN_QDRANT_API_KEY_ENV];
  return value === undefined || value.trim() === "" ? {} : { destinationApiKey: value };
}
