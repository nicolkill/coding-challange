module.exports = {
  iterate: async (getLength, query, itemCall, QUERY_LIMIT = 50000) => {
    let chunk;
    let count = await getLength();
    let skip = 0;
    let limit = QUERY_LIMIT;

    while (count > 0) {
      if (count < QUERY_LIMIT) {
        limit = count;
      }

      chunk = await query(skip, limit);
      if (itemCall) {
        chunk.forEach((item) => itemCall(item));
      }

      count = count - limit;
      skip += limit;
    }
  }
};