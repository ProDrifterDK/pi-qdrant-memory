export class AdminPlanError extends Error {
  constructor(message = "admin plan is invalid or stale") {
    super(message);
    this.name = "AdminPlanError";
  }
}
