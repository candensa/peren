export default {
  async fetch(_request, env) {
    return env.AWS_BEDROCK.fetch("https://bedrock-runtime.us-east-1.amazonaws.com/model/example/invoke", {
      method: "POST",
      body: JSON.stringify({ prompt: "hello" }),
    });
  },
};
