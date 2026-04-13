/*tslint:disable:no-else-after-return*/

import { BaseStep, Field, StepInterface, ExpectedRecord } from '../core/base-step';
import { Step, FieldDefinition, StepDefinition, RecordDefinition } from '../proto/cog_pb';

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

  async executeStep(step: Step) {
    const stepData: any = step.getData().toJavaScript();
    const exportLimitMB = stepData.exportLimit || 500;
    const exportLimitBytes = exportLimitMB * 1024 * 1024;
    const previousUsageBytes = (stepData.previousUsageMB || 0) * 1024 * 1024;

    try {
      // Get all export jobs from the different bulk API endpoints
      const leadJobsResponse = await this.client.getBulkExportLeadJobs();
      const activityJobsResponse = await this.client.getBulkExportActivityJobs();
      const programMemberJobsResponse = await this.client.getBulkExportProgramMemberJobs();
      const customObjectTypesResponse = await this.client.getCustomObjectTypes();

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

      // Process jobs from all endpoints
      if (leadJobsResponse.result) {
        processJobs(leadJobsResponse.result);
      }
      if (activityJobsResponse.result) {
        processJobs(activityJobsResponse.result);
      }
      if (programMemberJobsResponse.result) {
        processJobs(programMemberJobsResponse.result);
      }

      // Process custom object export jobs for each custom object type
      if (customObjectTypesResponse && customObjectTypesResponse.result) {
        for (const customObjectType of customObjectTypesResponse.result) {
          const customObjectJobsResponse = await this.client.getBulkExportCustomObjectJobs(customObjectType.name);
          if (customObjectJobsResponse && customObjectJobsResponse.result) {
            processJobs(customObjectJobsResponse.result);
          }
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

      // Always output the combined total so the next step's {{marketo.bulkExports.bulkApiUsage}}
      // token carries the running accumulated total across all chained users.
      const record = this.keyValue('bulkExports', 'Checked Bulk API Usage', { bulkApiUsage: parseFloat(combinedMBUsed) });

      if (combinedBytes < (0.9 * exportLimitBytes)) {
        return this.pass(passMessage, passArgs, [record]);
      }
      return this.fail(failMessage, failArgs, [record]);
    } catch (e) {
      return this.error('There was a problem checking the Bulk API Usage: %s', [e.toString()]);
    }
  }
}

export { CheckBulkApiUsageStep as Step };
