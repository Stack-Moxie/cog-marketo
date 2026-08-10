import * as Marketo from 'node-marketo-rest';

export class StatsAwareMixin {
  client: Marketo;
  delayInSeconds;

  public async getDailyApiUsage() {
    if (this.delayInSeconds > 0) {
      await this.delay(this.delayInSeconds);
    }
    return await this.client._connection.get('/v1/stats/usage.json');
  }

  public async getWeeklyApiUsage() {
    if (this.delayInSeconds > 0) {
      await this.delay(this.delayInSeconds);
    }
    return await this.client._connection.get('/v1/stats/usage/last7days.json');
  }

  /**
   * The client is configured against `<endpoint>/rest`, but the bulk API lives at
   * `<endpoint>/bulk`. The strip is anchored to the end of the string so that hosts
   * legitimately containing "rest" (every *.mktorest.com subscription) and endpoints
   * saved with a trailing slash both resolve to a well-formed URL.
   */
  public bulkUrl(path: string): string {
    const endpoint: string = this.client._connection._options.endpoint;
    const baseEndpoint = endpoint.replace(/\/rest\/*$/, '').replace(/\/+$/, '');
    return `${baseEndpoint}/bulk/v1/${path}`;
  }

  public async getBulkExportLeadJobs() {
    if (this.delayInSeconds > 0) {
      await this.delay(this.delayInSeconds);
    }
    return await this.client._connection.get(this.bulkUrl('leads/export.json'), { query: { _method: 'GET' } });
  }

  public async getBulkExportActivityJobs() {
    if (this.delayInSeconds > 0) {
      await this.delay(this.delayInSeconds);
    }
    return await this.client._connection.get(this.bulkUrl('activities/export.json'), { query: { _method: 'GET' } });
  }

  public async getBulkExportProgramMemberJobs() {
    if (this.delayInSeconds > 0) {
      await this.delay(this.delayInSeconds);
    }
    return await this.client._connection.get(this.bulkUrl('program/members/export.json'), { query: { _method: 'GET' } });
  }

  public async getCustomObjectTypes() {
    if (this.delayInSeconds > 0) {
      await this.delay(this.delayInSeconds);
    }
    return await this.client._connection.get('/v1/customobjects.json');
  }

  public async getBulkExportCustomObjectJobs(apiName: string) {
    if (this.delayInSeconds > 0) {
      await this.delay(this.delayInSeconds);
    }
    return await this.client._connection.get(this.bulkUrl(`customobjects/${apiName}/export.json`), { query: { _method: 'GET' } });
  }

  public async delay(seconds: number) {
    return new Promise(resolve => setTimeout(resolve, seconds * 1000));
  }
}
