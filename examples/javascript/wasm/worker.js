import compiled from './answer.wasm';

const instance = new WebAssembly.Instance(compiled);

export default {
  fetch() {
    return Response.json({ answer: instance.exports.answer() });
  },
};
