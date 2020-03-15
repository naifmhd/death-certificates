exports.subscribe = pubsubMessage => {
    // Print out the data from Pub/Sub, to prove that it worked
    console.log(Buffer.from(pubsubMessage.data, 'base64').toString());
  };