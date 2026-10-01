export default {
  async alarm() {
    console.log("alarm dispatched");
  },

  fetch() {
    return new Response("alarm() runs when the node dispatches it\n");
  },
};
