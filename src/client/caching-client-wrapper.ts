import { ClientWrapper } from '../client/client-wrapper';
import { promisify } from 'util';
class CachingClientWrapper {
  // cachePrefix is scoped to the specific scenario, request, and requestor
  public cachePrefix = `${this.idMap.scenarioId}${this.idMap.requestorId}${this.idMap.connectionId}`;

  constructor(private client: ClientWrapper, public redisClient: any, public idMap: any, public mailgunCredentials: any) {
    this.redisClient = redisClient;
    this.idMap = idMap;
    this.mailgunCredentials = mailgunCredentials;
  }

  // lead-aware methods
  // -------------------------------------------------------------------

  public async findLeadByEmail(email: string, justInCaseField: string = null, partitionId: number = null) {
    const cachekey = `Marketo|Lead|${email}|${this.cachePrefix}`;
    // check cache
    const stored = await this.getCache(cachekey);
    // if not there, call findLeadByEmail in lead-aware.ts
    if (stored) {
      return stored;
    } else {
      const newLead = await this.client.findLeadByEmail(email, justInCaseField, partitionId);
      if (newLead && newLead.success && newLead.result.length && Object.keys(newLead.result[0]).length > 8 && Object.keys(newLead.result[0]).length < 1000) {
        await this.setCache(cachekey, newLead);
      }
      return newLead;
    }
  }

  public async findLeadByField(field: string, value: string, justInCaseField: string = null, partitionId: number = null) {
    const cachekey = `Marketo|Lead|${value}|${this.cachePrefix}`;
    // check cache
    const stored = await this.getCache(cachekey);
    // if not there, call findLeadByField in lead-aware.ts
    if (stored) {
      return stored;
    } else {
      const newLead = await this.client.findLeadByField(field, value, justInCaseField, partitionId);
      if (newLead && newLead.success && newLead.result.length && Object.keys(newLead.result[0]).length > 8 && Object.keys(newLead.result[0]).length < 1000) {
        await this.setCache(cachekey, newLead);
      }
      return newLead;
    }
  }

  public async bulkFindLeadsByEmail(emails: [], justInCaseField: string = null, partitionId: number = 1) {
    await this.clearCache();
    return await this.client.bulkFindLeadsByEmail(emails, justInCaseField, partitionId);
  }

  public async bulkFindLeadsById(ids: [], justInCaseField: string = null, partitionId: number = null) {
    await this.clearCache();
    return await this.client.bulkFindLeadsById(ids, justInCaseField, partitionId);
  }

  public async createOrUpdateLead(lead: Record<string, any>, partitionId: number = 1) {
    await this.clearCache();
    return await this.client.createOrUpdateLead(lead, partitionId);
  }

  public async createLead(lead: Record<string, any>, partitionId: number = 1) {
    await this.clearCache();
    return await this.client.createLead(lead, partitionId);
  }

  public async updateLead(lead: Record<string, any>, lookupField: string, value: string, partitionId: number = 1) {
    await this.clearCache();
    return await this.client.updateLead(lead, lookupField, value, partitionId);
  }

  public async bulkCreateOrUpdateLead(leads: [], partitionId: number = 1) {
    await this.clearCache();
    return await this.client.bulkCreateOrUpdateLead(leads, partitionId);
  }

  public async bulkRemoveLeadsFromProgram(leads: [], programId: string, partitionId: number = null) {
    await this.clearCache();
    return await this.client.bulkRemoveLeadsFromProgram(leads, programId, partitionId);
  }

  public async bulkSetStatusToLeadsFromProgram(leads: [], programId: string, status: string, partitionId: number = null) {
    await this.clearCache();
    return await this.client.bulkSetStatusToLeadsFromProgram(leads, programId, status, partitionId);
  }

  public async deleteLeadById(leadId: number, email: string = null) {
    await this.clearCache();
    return await this.client.deleteLeadById(leadId);
  }

  public async describeLeadFields(email: string = '') {
    const cachekey = `Marketo|Description|${email}|${this.cachePrefix}`;
    // check cache
    const stored = await this.getCache(cachekey);
    // if not there, call describeLeadFields in lead-aware.ts
    if (stored) {
      return stored;
    } else {
      const newLeadDescription = await this.client.describeLeadFields();
      await this.setCache(cachekey, newLeadDescription);
      return newLeadDescription;
    }
  }

  public async mergeLeadsById(winningLead: string, losingLeads: string[]) {
    await this.clearCache();
    return await this.client.mergeLeadsById(winningLead, losingLeads);
  }

  // custom-object-aware methods
  // -------------------------------------------------------------------

  public async getCustomObject(customObjectName, email: string = null) {
    const cachekey = `Marketo|Object|${email}${customObjectName}|${this.cachePrefix}`;
    // check cache
    const stored = await this.getCache(cachekey);
    // if not there, call getCustomObject in custom-object-aware.ts
    if (stored) {
      return stored;
    } else {
      const newCustomObject = await this.client.getCustomObject(customObjectName);
      if (newCustomObject && newCustomObject.success && newCustomObject.result.length) {
        await this.setCache(cachekey, newCustomObject);
      }
      return newCustomObject;
    }
  }

  public async queryCustomObject(customObjectName, filterType, searchFields: any[], requestFields: string[] = [], email: string = '') {
    const cachekey = `Marketo|Query|${email}${customObjectName}|${this.cachePrefix}`;
    // check cache
    const stored = await this.getCache(cachekey);
    // if not there, call queryCustomObject in custom-object-aware.ts
    if (stored) {
      return stored;
    } else {
      const newCustomObjectQuery = await this.client.queryCustomObject(customObjectName, filterType, searchFields, requestFields);
      if (newCustomObjectQuery && newCustomObjectQuery.success && newCustomObjectQuery.result.length) {
        await this.setCache(cachekey, newCustomObjectQuery);
      }
      return newCustomObjectQuery;
    }
  }

  public async createOrUpdateCustomObject(customObjectName, customObject: Record<string, any>) {
    await this.clearCache();
    return await this.client.createOrUpdateCustomObject(customObjectName, customObject);
  }

  public async deleteCustomObjectById(customObjectName, customObjectGUID, email: string = '') {
    await this.clearCache();
    return await this.client.deleteCustomObjectById(customObjectName, customObjectGUID);
  }

  // smart-campaign-aware methods
  // -------------------------------------------------------------------
  // Campaigns will be cached with cacheKey = cachePrefix + 'Campaigns'

  public async getCampaigns() {
    const cachekey = `Marketo|Campaigns|${this.cachePrefix}`;
    // check cache
    const stored = await this.getCache(cachekey);
    // if not there, call getCampaigns in smart-campaign-aware.ts
    if (stored) {
      return stored;
    } else {
      const campaigns = await this.client.getCampaigns();
      if (campaigns && campaigns.length) {
        await this.setCache(cachekey, campaigns);
      }
      return campaigns;
    }
  }

  public async addLeadToSmartCampaign(campaignId: string, lead: Record<string, any>) {
    await this.clearCache();
    return await this.client.addLeadToSmartCampaign(campaignId, lead);
  }

  public async bulkAddLeadToSmartCampaign(campaignId: string, leadArray: Number[]) {
    await this.clearCache();
    return await this.client.bulkAddLeadToSmartCampaign(campaignId, leadArray);
  }

  // static-list-aware methods
  // -------------------------------------------------------------------
  // Static Lists will be cached with cacheKey = cachePrefix + 'Campaigns'

  public async findStaticListsByName(name: string) {
    return await this.client.findStaticListsByName(name);
  }

  public async findStaticListsById(id: string) {
    return await this.client.findStaticListsById(id);
  }

  public async addLeadToStaticList(listId: string, leadIds: string[]) {
    return await this.client.addLeadToStaticList(listId, leadIds);
  }

  public async removeLeadToStaticList(listId: string, leadIds: string[]) {
    return await this.client.removeLeadToStaticList(listId, leadIds);
  }

  public async findStaticListsMembershipByListId(id: string) {
    return await this.client.findStaticListsMembershipByListId(id);
  }

  // email-aware methods
  // -------------------------------------------------------------------
  // Campaigns will be cached with cacheKey = Marketo|Emails|cachePrefix

  public async getEmails() {
    const cachekey = `Marketo|Emails|${this.cachePrefix}`;
    // check cache
    const stored = await this.getCache(cachekey);
    // if not there, call getCampaigns in smart-campaign-aware.ts
    if (stored) {
      return stored;
    } else {
      const emails = await this.client.getEmails();
      if (emails && emails.length) {
        await this.setCache(cachekey, emails);
      }
      return emails;
    }
  }

  public async getEmailByName(name) {
    return await this.client.getEmailByName(name);
  }

  // all non-cached functions, just referencing the original function
  // -------------------------------------------------------------------

  public async getActivityTypes() {
    return await this.client.getActivityTypes();
  }

  public async getActivityPagingToken(sinceDate) {
    return await this.client.getActivityPagingToken(sinceDate);
  }

  public async getActivitiesByLeadId(nextPageToken, leadId, activityId) {
    return await this.client.getActivitiesByLeadId(nextPageToken, leadId, activityId);
  }

  public getNotificationInbox() {
    return this.client.getNotificationInbox();
  }

  public async getNotificationMessages(inbox: string) {
    return await this.client.getNotificationMessages(inbox);
  }

  public async getDailyApiUsage() {
    return await this.client.getDailyApiUsage();
  }

  public async getWeeklyApiUsage() {
    return await this.client.getWeeklyApiUsage();
  }

  public async getBulkExportLeadJobs() {
    return await this.client.getBulkExportLeadJobs();
  }

  public async getBulkExportActivityJobs() {
    return await this.client.getBulkExportActivityJobs();
  }

  public async getBulkExportProgramMemberJobs() {
    return await this.client.getBulkExportProgramMemberJobs();
  }

  public async getCustomObjectTypes() {
    return await this.client.getCustomObjectTypes();
  }

  public async getBulkExportCustomObjectJobs(apiName: string) {
    return await this.client.getBulkExportCustomObjectJobs(apiName);
  }

  public async createProgram(program) {
    return await this.client.createProgram(program);
  }

  public async updateProgram(id: string, programUpdateString: string) {
    return await this.client.updateProgram(id, programUpdateString);
  }

  public async getPrograms() {
    return await this.client.getPrograms();
  }

  public async findProgramsByName(name: string) {
    return await this.client.findProgramsByName(name);
  }

  public async findProgramsById(id: string) {
    return await this.client.findProgramsById(id);
  }

  public async deleteProgramById(id: string) {
    return await this.client.deleteProgramById(id);
  }

  public async getProgramMembersByFilterValue(programId: string, field: string, fieldValue: string, fields: string[] = []) {
    return await this.client.getProgramMembersByFilterValue(programId, field, fieldValue, fields);
  }

  public async getProgramMembersFields() {
    return await this.client.getProgramMembersFields();
  }

  public async getFoldersById(id: string) {
    return await this.client.getFoldersById(id);
  }

  public async sendSampleEmail(emailId, email) {
    return await this.client.sendSampleEmail(emailId, email);
  }

  // Redis methods for get, set, and delete
  // -------------------------------------------------------------------

  // Async getter/setter
  public getAsync = promisify(this.redisClient.get).bind(this.redisClient);
  public setAsync = promisify(this.redisClient.setex).bind(this.redisClient);
  public delAsync = promisify(this.redisClient.del).bind(this.redisClient);

  public async getCache(key: string) {
    try {
      const stored = await this.getAsync(key);
      if (stored) {
        return JSON.parse(stored);
      }
      return null;
    } catch (err) {
      console.log(err);
    }
  }

  public async setCache(key: string, value: any) {
    try {
      // arrOfKeys will store an array of all cache keys used in this scenario run, so it can be cleared easily
      const arrOfKeys = await this.getCache(`cachekeys|${this.cachePrefix}`) || [];
      arrOfKeys.push(key);
      await this.setAsync(key, 55, JSON.stringify(value));
      await this.setAsync(`cachekeys|${this.cachePrefix}`, 55, JSON.stringify(arrOfKeys));
    } catch (err) {
      console.log(err);
    }
  }

  public async clearCache() {
    try {
      // clears all the cachekeys used in this scenario run
      const keysToDelete = await this.getCache(`cachekeys|${this.cachePrefix}`) || [];
      if (keysToDelete.length) {
        keysToDelete.forEach(async (key: string) => await this.delAsync(key));
      }
      await this.setAsync(`cachekeys|${this.cachePrefix}`, 55, '[]');
    } catch (err) {
      console.log(err);
    }
  }

}

export { CachingClientWrapper as CachingClientWrapper };
