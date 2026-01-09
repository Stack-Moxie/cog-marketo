/*tslint:disable:no-else-after-return*/

import { BaseStep, Field, StepInterface, ExpectedRecord } from '../core/base-step';
import { Step, FieldDefinition, StepDefinition, RecordDefinition, StepRecord } from '../proto/cog_pb';

export class CreateOrUpdateLeadByFieldStep extends BaseStep implements StepInterface {

  protected stepName: string = 'Create or update a Marketo lead';
  protected stepExpression: string = 'create or update a marketo lead';
  protected stepType: StepDefinition.Type = StepDefinition.Type.ACTION;
  protected actionList: string[] = ['create', 'update'];
  protected targetObject: string = 'Lead';
  protected expectedFields: Field[] = [
    {
      field: 'partitionId',
      type: FieldDefinition.Type.NUMERIC,
      optionality: FieldDefinition.Optionality.OPTIONAL,
      description: 'ID of partition the lead will be created',
    },
    {
      field: 'lead',
      type: FieldDefinition.Type.MAP,
      description: 'A map of field names to field values',
    },
  ];
  protected expectedRecords: ExpectedRecord[] = [{
    id: 'lead',
    type: RecordDefinition.Type.KEYVALUE,
    fields: [{
      field: 'id',
      type: FieldDefinition.Type.NUMERIC,
      description: "Lead's Marketo ID",
    }, {
      field: 'email',
      type: FieldDefinition.Type.NUMERIC,
      description: "Lead's Email",
    }],
    dynamicFields: true,
  }];

  async executeStep(step: Step) {
    const stepData: any = step.getData().toJavaScript();
    const partitionId = stepData.partitionId || 1;
    const lead = stepData.lead;
    const updateMostRecentMatch: boolean = stepData.updateMostRecentMatch || false;

    try {
      const data: any = await this.client.createOrUpdateLead(lead, partitionId);
      if (data.success && data.result && data.result[0] && data.result[0].status !== 'skipped') {
        const createdLead: any = await this.client.findLeadByEmail(lead.email, null, partitionId);
        const record = this.createRecord(createdLead.result[0]);
        const passingRecord = this.createPassingRecord(createdLead.result[0], Object.keys(lead));
        const orderedRecord = this.createOrderedRecord(createdLead.result[0], stepData['__stepOrder']);
        return this.pass(
          'Successfully created or updated lead %s with status %s',
          [lead.email, data.result[0].status],
          [record, passingRecord, orderedRecord],
        );
      } else if (data && data.error && !data.error.partition) {
        return this.fail('There is no Partition with id %s', [
          partitionId,
        ]);
      } else {
        if (data.result && data.result[0] && data.result[0].reasons && data.result[0].reasons[0]) {
          const errorMessage = data.result[0].reasons[0].message;

          // Check if this is the "Multiple lead match lookup criteria" error and updateMostRecentMatch is true
          if (updateMostRecentMatch && errorMessage && errorMessage.toLowerCase().includes('multiple lead match lookup criteria')) {
            try {
              // Find all leads matching the email
              const matchingLeads: any = await this.client.findLeadByEmail(lead.email, null, partitionId);

              if (matchingLeads.success && matchingLeads.result && matchingLeads.result.length > 0) {
                // Sort by updatedAt descending to get the most recent lead
                const sortedLeads = matchingLeads.result.sort((a, b) => {
                  const dateA = new Date(a.updatedAt || a.createdAt || 0);
                  const dateB = new Date(b.updatedAt || b.createdAt || 0);
                  return dateB.getTime() - dateA.getTime();
                });

                const mostRecentLead = sortedLeads[0];

                // Update the most recent lead by ID
                // Create a new lead object with the ID to ensure proper update
                const leadWithId = { ...lead, id: mostRecentLead.id };
                const updateData: any = await this.client.updateLead(leadWithId, 'id', mostRecentLead.id.toString(), partitionId);

                if (updateData.success && updateData.result && updateData.result[0] && updateData.result[0].status !== 'skipped') {
                  // Fetch the updated lead to get all current data
                  const updatedLead: any = await this.client.findLeadByEmail(lead.email, null, partitionId);
                  const finalLead = updatedLead.result.find(l => l.id === mostRecentLead.id) || updatedLead.result[0];

                  const record = this.createRecord(finalLead);
                  const passingRecord = this.createPassingRecord(finalLead, Object.keys(lead));
                  const orderedRecord = this.createOrderedRecord(finalLead, stepData['__stepOrder']);

                  return this.pass(
                    'Successfully updated most recent lead (ID: %s) from %d matching leads',
                    [mostRecentLead.id, matchingLeads.result.length],
                    [record, passingRecord, orderedRecord],
                  );
                } else {
                  return this.fail('Unable to update most recent lead: %s', [
                    updateData.result && updateData.result[0] && updateData.result[0].reasons
                      ? updateData.result[0].reasons[0].message
                      : 'Update failed',
                  ]);
                }
              } else {
                return this.fail('Unable to find matching leads for: %s', [lead.email]);
              }
            } catch (resolveError) {
              return this.error('Error resolving multiple lead match: %s', [resolveError.toString()]);
            }
          }

          return this.fail('Unable to create or update lead: %s', [
            errorMessage,
          ]);
        } else {
          return this.fail('Unable to create or update lead: %s', [
            `status was ${data.result[0].status}`,
          ]);
        }
      }
    } catch (e) {
      return this.error('There was an error creating or updating leads in Marketo: %s', [
        e.toString(),
      ]);
    }
  }

  public createRecord(lead): StepRecord {
    return this.keyValue('lead', 'Created Lead', lead);
  }

  public createPassingRecord(data, fields): StepRecord {
    const filteredData = {};
    if (data) {
      Object.keys(data).forEach((key) => {
        if (fields.includes(key)) {
          filteredData[key] = data[key];
        }
      });
    }
    return this.keyValue('exposeOnPass:lead', 'Created Lead', filteredData);
  }

  public createOrderedRecord(lead, stepOrder = 1): StepRecord {
    return this.keyValue(`lead.${stepOrder}`, `Created Lead from Step ${stepOrder}`, lead);
  }

}

export { CreateOrUpdateLeadByFieldStep as Step };
