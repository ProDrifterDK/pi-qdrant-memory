const PREFIX = "PI_QDRANT_MEMORY_";
const ADMIN_SUFFIX = ["ADMIN", "_QDRANT_API_KEY"].join("");
export const ADMIN_QDRANT_API_KEY_ENV = `${PREFIX}${ADMIN_SUFFIX}`;
export function loadAdminProcessSecrets(env) {
    const value = env[ADMIN_QDRANT_API_KEY_ENV];
    return value === undefined || value.trim() === "" ? {} : { destinationApiKey: value };
}
//# sourceMappingURL=secrets.js.map