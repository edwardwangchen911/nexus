export class NexusBffError extends Error {
  constructor(
    readonly status: number,
    readonly code: "UNAUTHENTICATED" | "FORBIDDEN" | "VALIDATION_ERROR" | "INTERNAL_ERROR",
    message: string,
  ) {
    super(message);
  }
}

export function safeBffErrorResponse(error: unknown): Response {
  if (error instanceof NexusBffError) {
    return Response.json({ error: { code: error.code, message: error.message } }, { status: error.status });
  }
  return Response.json(
    { error: { code: "INTERNAL_ERROR", message: "The request could not be completed." } },
    { status: 500 },
  );
}
