/*tslint:disable:no-else-after-return*/

import { BaseStep, Field, StepInterface, ExpectedRecord } from '../core/base-step';
import { Step, FieldDefinition, StepDefinition, RecordDefinition } from '../proto/cog_pb';

interface EndpointResult {
  label: string;
  response?: any;
  error?: string;
}

export class CheckBulkApiUsageStep extends BaseStep implements StepInterface {

  protected stepName: string = 'Check daily Marketo Bulk API usage';
  protected stepExpression: string = 'there should be less than 90% usage of your daily bulk API limit';
  protected stepType: StepDefinition.Type = StepDefinition.Type.VALIDATION;
  protected actionList: string[] = ['check'];
  protected targetObject: string = 'Bulk API Usage';
  protected expectedFields: Field[] = [{
    field: 'exportLimit',
    type: FieldDefinition.Type.NUMERIC,
    optionality: FieldDefinition.Optionality.OPTIONAL,
    description: 'Your daily bulk export limit in MB (default: 500)',
  }, {
    field: 'previousUsageMB',
    type: FieldDefinition.Type.NUMERIC,
    optionality: FieldDefinition.Optionality.OPTIONAL,
    description: 'MB used by previous API users (use {{marketo.bulkExports.bulkApiUsage}} to chain steps across multiple users)',
  }];
  protected expectedRecords: ExpectedRecord[] = [{
    id: 'bulkExports',
    type: RecordDefinition.Type.KEYVALUE,
    fields: [{
      field: 'bulkApiUsage',
      type: FieldDefinition.Type.NUMERIC,
      description: 'Daily Bulk API Usage in MB',
    }],
    dynamicFields: false,
  }];

  /**
   * node-marketo-rest's HttpError carries the HTTP status on `code` and the Marketo
   * error array on `errors`, but its `message` falls back to the useless literal
   * "Unknown Marketo error" whenever the response body isn't Marketo-shaped JSON
   * (gateway 5xx pages, for instance). Calling toString() throws away exactly the
   * detail needed to tell a quota problem apart from an upstream outage.
   */
  private describeError(e: any): string {
    if (!e) {
      return 'unknown error';
    }

    const parts: string[] = [];

    if (typeof e.code === 'number') {
      parts.push(`HTTP ${e.code}`);
    } else if (e.code) {
      parts.push(`${e.code}`);
    }

    if (Array.isArray(e.errors) && e.errors.length) {
      parts.push(e.errors.map(err => `Marketo ${err.code}: ${err.message}`).join('; '));
    } else {
      parts.push(e.message || e.toString());
    }

    return parts.join(' — ');
  }

  private async attempt(label: string, call: () => Promise<any>): Promise<EndpointResult> {
    try {
      return { label, response: await call() };
    } catch (e) {
      return { label, error: this.describeError(e) };
    }
  }

  async executeStep(step: Step) {
    const stepData: any = step.getData().toJavaScript();
    const exportLimitMB = stepData.exportLimit || 500;
    const exportLimitBytes = exportLimitMB * 1024 * 1024;
    const previousUsageBytes = (stepData.previousUsageMB || 0) * 1024 * 1024;

    try {
      // Each endpoint is called independently. A transient failure on one of them must
      // not discard the usage already gathered from the others, so failures are collected
      // and named in the outcome rather than aborting the whole check.
      const failures: string[] = [];
      const record = (endpoint: EndpointResult, process: (jobs: any[]) => void) => {
        if (endpoint.error) {
          failures.push(`${endpoint.label} (${endpoint.error})`);
        } else if (endpoint.response && endpoint.response.result) {
          process(endpoint.response.result);
        }
      };

      // Calculate today's total usage by summing fileSize from all completed jobs that
      // finished today in Central Time. Marketo's daily quota resets at midnight Central
      // Time, so this must match the timezone Marketo uses — not the server's local time.
      const todayCentralStr = new Date().toLocaleDateString('en-US', { timeZone: 'America/Chicago' });

      let totalBytesToday = 0;
      let jobCount = 0;

      // Helper function to process jobs from each endpoint
      const processJobs = (jobs: any[]) => {
        if (!jobs || !Array.isArray(jobs)) {
          return;
        }

        jobs.forEach((job) => {
          // Only count completed jobs whose finishedAt falls on today in Central Time.
          // Using finishedAt (not createdAt) matches how Marketo attributes quota usage,
          // and using Central Time matches Marketo's midnight quota reset.
          if (!job.finishedAt || job.status !== 'Completed' || !job.fileSize) {
            return;
          }
          const jobFinishedCentralStr = new Date(job.finishedAt).toLocaleDateString('en-US', { timeZone: 'America/Chicago' });
          if (jobFinishedCentralStr === todayCentralStr) {
            totalBytesToday += job.fileSize;
            jobCount += 1;
          }
        });
      };

      record(await this.attempt('lead exports', () => this.client.getBulkExportLeadJobs()), processJobs);
      record(await this.attempt('activity exports', () => this.client.getBulkExportActivityJobs()), processJobs);
      record(await this.attempt('program member exports', () => this.client.getBulkExportProgramMemberJobs()), processJobs);

      const customObjectTypes = await this.attempt('custom object types', () => this.client.getCustomObjectTypes());
      if (customObjectTypes.error) {
        failures.push(`${customObjectTypes.label} (${customObjectTypes.error})`);
      } else if (customObjectTypes.response && customObjectTypes.response.result) {
        for (const customObjectType of customObjectTypes.response.result) {
          record(
            await this.attempt(
              `custom object exports for ${customObjectType.name}`,
              () => this.client.getBulkExportCustomObjectJobs(customObjectType.name),
            ),
            processJobs,
          );
        }
      }

      // Convert to MB for display. If previousUsageBytes is set, the combined total is
      // what gets compared against the limit and output as the token for the next step.
      const thisUserMBUsed = (totalBytesToday / (1024 * 1024)).toFixed(2);
      const combinedBytes = totalBytesToday + previousUsageBytes;
      const combinedMBUsed = (combinedBytes / (1024 * 1024)).toFixed(2);
      const percentUsage = ((combinedBytes / exportLimitBytes) * 100).toFixed(2);
      const isAccumulating = previousUsageBytes > 0;

      const passMessage = isAccumulating
        ? 'This user has used %s MB today. Combined with previous users, total usage is %s MB of your %d MB daily limit (%s%%). Based on %d completed export job(s) today.'
        : 'You have used %s MB of your %d MB daily bulk export limit, which is %s%% of your quota. This is based on %d completed export job(s) today.';
      const failMessage = isAccumulating
        ? 'This user has used %s MB today. Combined with previous users, total usage is %s MB of your %d MB daily limit (%s%%). You are approaching or have exceeded your daily limit. Based on %d completed export job(s) today.'
        : 'You have used %s MB of your %d MB daily bulk export limit, which is %s%% of your quota. This is based on %d completed export job(s) today. You are approaching or have exceeded your daily limit.';

      const passArgs = isAccumulating
        ? [thisUserMBUsed, combinedMBUsed, exportLimitMB, percentUsage, jobCount]
        : [combinedMBUsed, exportLimitMB, percentUsage, jobCount];
      const failArgs = isAccumulating
        ? [thisUserMBUsed, combinedMBUsed, exportLimitMB, percentUsage, jobCount]
        : [combinedMBUsed, exportLimitMB, percentUsage, jobCount];

      const overLimit = combinedBytes >= (0.9 * exportLimitBytes);

      // An endpoint that didn't answer can only have added usage, never removed it, so a
      // breach measured from partial data is still a real breach and worth reporting.
      if (overLimit) {
        return this.fail(
          failures.length ? `${failMessage} Actual usage may be higher: %s did not respond.` : failMessage,
          failures.length ? [...failArgs, failures.join('; ')] : failArgs,
          // Always output the combined total so the next step's {{marketo.bulkExports.bulkApiUsage}}
          // token carries the running accumulated total across all chained users.
          [this.keyValue('bulkExports', 'Checked Bulk API Usage', { bulkApiUsage: parseFloat(combinedMBUsed) })],
        );
      }

      // Under the threshold on incomplete data proves nothing — the missing endpoints
      // could hold the rest of the quota. No record is emitted, so a chained step fails
      // loudly on an unresolved token rather than silently inheriting an undercount.
      if (failures.length) {
        return this.error(
          'Could not complete the Bulk API Usage check: %s. Only %s MB across %d completed export job(s) could be counted, so the daily total is incomplete.',
          [failures.join('; '), combinedMBUsed, jobCount],
        );
      }

      return this.pass(
        passMessage,
        passArgs,
        [this.keyValue('bulkExports', 'Checked Bulk API Usage', { bulkApiUsage: parseFloat(combinedMBUsed) })],
      );
    } catch (e) {
      return this.error('There was a problem checking the Bulk API Usage: %s', [this.describeError(e)]);
    }
  }
}

export { CheckBulkApiUsageStep as Step };
