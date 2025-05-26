import { isMainThread } from "node:worker_threads";

// What we're trying to do here:
// - Often, when printing, converting to string, or JSON.stringify-ing an error object, it's often incosistent, unhelpful, and unstructured:
//   - The actual type isn't shown, because the subclass didn't override the `name` prototype property.
//   - The message doesn't contain important information found on the object's own properties.
//   - The stack trace isn't collected by String() or JSON.stringify(), so many places where we use these to collect the error for later debugging inadvertently misses out on this important piece.
//   - The `cause` is forgotten because, like `stack` and `name` (mentioned before), it's on the prototype. There's a common theme here: forgetting that a lot of important things are not in the message, and not in its own properties but on the prototype.
//   - It isn't clear that the error, or all its information (as described above), is "portably transportable" e.g. JSON.stringify() captures all this information.
const errorToObject = (err: Error): any => ({
  data: { ...err },
  message: err.message,
  name: err.name,
  cause: err.cause instanceof Error ? errorToObject(err.cause) : err.cause,
  trace: err.stack,
  type: err.constructor?.name,
});

export const setUpUncaughtExceptionHandler = () => {
  if (!isMainThread) {
    return;
  }
  const handleError = (err: Error) => {
    console.error(
      JSON.stringify({
        ...errorToObject(err),
        level: "CRITICAL",
        unhandled: true,
      }),
    );
    process.exit(1);
  };
  process.on("uncaughtException", handleError);
  process.on("unhandledRejection", handleError);
  process.on("warning", (warning) => {
    console.warn(
      JSON.stringify({
        ...errorToObject(warning),
        level: "WARN",
      }),
    );
  });
};
