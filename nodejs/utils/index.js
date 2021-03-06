const db = require('../db')

module.exports = {
  saveItem: async (item) => {
    const col = await db.getCollection()
    item['task_id'] = process.env['CRAWLAB_TASK_ID']
    const isDedup = process.env['CRAWLAB_IS_DEDUP']
    const dedupField = process.env['CRAWLAB_DEDUP_FIELD']
    const dedupMethod = process.env['CRAWLAB_DEDUP_METHOD']
    try {
      let result;
      if (isDedup) {
        if (dedupMethod === 'overwrite') {
          const query = {}
          query[dedupField] = item[dedupField]
          await col.removeOne(query)
          result = await col.insertOne(item)
        } else if (dedupMethod === 'ignore') {
          result = await col.insertOne(item)
        } else {
          result = await col.insertOne(item)
        }
      } else {
        result = await col.insertOne(item)
      }
      return result;
    } catch (e) {
      throw e;
    }
  },
  find: async (query) => {
    try {
      const col = await db.getCollection();
      return await col.find(query).toArray();
    } catch (e) {
      throw e;
    }
  },
  update: async (query, update) => {
    try {
      const col = await db.getCollection();
      return (await col.updateMany(query, update)).modifiedCount;
    } catch (e) {
      throw e;
    }
  },
  delete: async (query) => {
    try {
      const col = await db.getCollection();
      return (await col.deleteMany(query)).deletedCount;
    } catch (e) {
      throw e;
    }
  },
  findLatest: async (queryField) => {
    try {
      const col = await db.getCollection()
      const sort = {};
      sort[queryField] = -1;
      const result = await col.find().sort(sort).limit(1).toArray();
      if (result.length==1) return result[0];
      else throw new Error('Not Found');
    } catch (e) {
      throw e;
    }
  },
  close: async () => {
    const client = await db.getClient()
    await client.close()
  }
}
