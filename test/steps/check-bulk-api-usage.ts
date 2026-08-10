import { Struct } from 'google-protobuf/google/protobuf/struct_pb';
import * as chai from 'chai';
import { default as sinon } from 'ts-sinon';
import * as sinonChai from 'sinon-chai';
import 'mocha';

import { Step as ProtoStep, StepDefinition, RunStepResponse } from '../../src/proto/cog_pb';
import { Step } from '../../src/steps/check-bulk-api-usage';

chai.use(sinonChai);

describe('CheckBulkApiUsageStep', () => {
  const expect = chai.expect;
  let protoStep: ProtoStep;
  let stepUnderTest: Step;
  let clientWrapperStub: any;

  beforeEach(() => {
    protoStep = new ProtoStep();
    clientWrapperStub = sinon.stub();
    clientWrapperStub.getBulkExportLeadJobs = sinon.stub();
    clientWrapperStub.getBulkExportActivityJobs = sinon.stub();
    clientWrapperStub.getBulkExportProgramMemberJobs = sinon.stub();
    clientWrapperStub.getCustomObjectTypes = sinon.stub();
    clientWrapperStub.getBulkExportCustomObjectJobs = sinon.stub();
    stepUnderTest = new Step(clientWrapperStub);
  });

  it('should return expected step metadata', () => {
    const stepDef: StepDefinition = stepUnderTest.getDefinition();
    expect(stepDef.getStepId()).to.equal('CheckBulkApiUsageStep');
    expect(stepDef.getName()).to.equal('Check daily Marketo Bulk API usage');
    expect(stepDef.getExpression()).to.equal('there should be less than 90% usage of your daily bulk API limit');
    expect(stepDef.getType()).to.equal(StepDefinition.Type.VALIDATION);
  });

  it('should respond with success if bulk API usage is less than 90% of the daily limit', async () => {
    const expectedLimit: number = 500; // 500MB default
    // Use current moment — always "today" in any timezone
    const nowISO = new Date().toISOString();

    protoStep.setData(Struct.fromJavaScript({
      exportLimit: expectedLimit,
    }));

    // Mock lead export jobs - 100MB used today
    clientWrapperStub.getBulkExportLeadJobs.returns(Promise.resolve({
      success: true,
      result: [
        {
          finishedAt: nowISO,
          status: 'Completed',
          fileSize: 100 * 1024 * 1024, // 100MB
        },
      ],
    }));

    // Mock activity export jobs - 50MB used today
    clientWrapperStub.getBulkExportActivityJobs.returns(Promise.resolve({
      success: true,
      result: [
        {
          finishedAt: nowISO,
          status: 'Completed',
          fileSize: 50 * 1024 * 1024, // 50MB
        },
      ],
    }));

    // Mock program member export jobs - empty
    clientWrapperStub.getBulkExportProgramMemberJobs.returns(Promise.resolve({
      success: true,
      result: [],
    }));

    clientWrapperStub.getCustomObjectTypes.returns(Promise.resolve({ success: true, result: [] }));

    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.PASSED);
  });

  it('should respond with a failure if bulk API usage is more than 90% of the daily limit', async () => {
    const expectedLimit: number = 500;
    const nowISO = new Date().toISOString();

    protoStep.setData(Struct.fromJavaScript({
      exportLimit: expectedLimit,
    }));

    // Mock lead export jobs - 400MB used today
    clientWrapperStub.getBulkExportLeadJobs.returns(Promise.resolve({
      success: true,
      result: [
        {
          finishedAt: nowISO,
          status: 'Completed',
          fileSize: 400 * 1024 * 1024, // 400MB
        },
      ],
    }));

    // Mock activity export jobs - 100MB used today
    clientWrapperStub.getBulkExportActivityJobs.returns(Promise.resolve({
      success: true,
      result: [
        {
          finishedAt: nowISO,
          status: 'Completed',
          fileSize: 100 * 1024 * 1024, // 100MB
        },
      ],
    }));

    // Mock program member export jobs - empty
    clientWrapperStub.getBulkExportProgramMemberJobs.returns(Promise.resolve({
      success: true,
      result: [],
    }));

    clientWrapperStub.getCustomObjectTypes.returns(Promise.resolve({ success: true, result: [] }));

    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.FAILED);
  });

  it('should only count jobs from today', async () => {
    const expectedLimit: number = 500;
    // Current moment is always "today" in any timezone
    const nowISO = new Date().toISOString();
    // 48 hours ago is always "before today" in any timezone
    const twoDaysAgoISO = new Date(Date.now() - 48 * 60 * 60 * 1000).toISOString();

    protoStep.setData(Struct.fromJavaScript({
      exportLimit: expectedLimit,
    }));

    // Mock lead export jobs - one from today (50MB), one from two days ago (400MB)
    clientWrapperStub.getBulkExportLeadJobs.returns(Promise.resolve({
      success: true,
      result: [
        {
          finishedAt: nowISO,
          status: 'Completed',
          fileSize: 50 * 1024 * 1024, // 50MB today
        },
        {
          finishedAt: twoDaysAgoISO,
          status: 'Completed',
          fileSize: 400 * 1024 * 1024, // 400MB two days ago (should not count)
        },
      ],
    }));

    clientWrapperStub.getBulkExportActivityJobs.returns(Promise.resolve({
      success: true,
      result: [],
    }));

    clientWrapperStub.getBulkExportProgramMemberJobs.returns(Promise.resolve({
      success: true,
      result: [],
    }));

    clientWrapperStub.getCustomObjectTypes.returns(Promise.resolve({ success: true, result: [] }));

    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    // Should pass because only 50MB from today counts
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.PASSED);
  });

  it('should only count completed jobs', async () => {
    const expectedLimit: number = 500;
    const nowISO = new Date().toISOString();

    protoStep.setData(Struct.fromJavaScript({
      exportLimit: expectedLimit,
    }));

    // Mock lead export jobs - one completed (50MB), one queued (400MB, no finishedAt yet)
    clientWrapperStub.getBulkExportLeadJobs.returns(Promise.resolve({
      success: true,
      result: [
        {
          finishedAt: nowISO,
          status: 'Completed',
          fileSize: 50 * 1024 * 1024, // 50MB completed
        },
        {
          status: 'Queued',
          fileSize: 400 * 1024 * 1024, // 400MB queued (should not count — no finishedAt)
        },
      ],
    }));
    
    clientWrapperStub.getBulkExportActivityJobs.returns(Promise.resolve({
      success: true,
      result: [],
    }));

    clientWrapperStub.getBulkExportProgramMemberJobs.returns(Promise.resolve({
      success: true,
      result: [],
    }));

    clientWrapperStub.getCustomObjectTypes.returns(Promise.resolve({ success: true, result: [] }));

    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    // Should pass because only 50MB from completed jobs counts
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.PASSED);
  });

  it('should handle empty results', async () => {
    const expectedLimit: number = 500;
    
    protoStep.setData(Struct.fromJavaScript({
      exportLimit: expectedLimit,
    }));
    
    clientWrapperStub.getBulkExportLeadJobs.returns(Promise.resolve({
      success: true,
      result: [],
    }));
    
    clientWrapperStub.getBulkExportActivityJobs.returns(Promise.resolve({
      success: true,
      result: [],
    }));

    clientWrapperStub.getBulkExportProgramMemberJobs.returns(Promise.resolve({
      success: true,
      result: [],
    }));

    clientWrapperStub.getCustomObjectTypes.returns(Promise.resolve({ success: true, result: [] }));

    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    // Should pass with 0MB usage
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.PASSED);
  });

  it('should respond with an error if the marketo client throws an error', async () => {
    const httpError: any = new Error('Unknown Marketo error');
    httpError.name = 'HttpError';
    httpError.code = 502;
    clientWrapperStub.getBulkExportLeadJobs.rejects(httpError);
    clientWrapperStub.getBulkExportActivityJobs.returns(Promise.resolve({ success: true, result: [] }));
    clientWrapperStub.getBulkExportProgramMemberJobs.returns(Promise.resolve({ success: true, result: [] }));
    clientWrapperStub.getCustomObjectTypes.returns(Promise.resolve({ success: true, result: [] }));
    protoStep.setData(Struct.fromJavaScript({
      exportLimit: 500,
    }));
    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.ERROR);
    const args = response.getMessageArgsList().map(a => a.toJavaScript()).join(' ');
    expect(args).to.contain('lead exports');
    expect(args).to.contain('HTTP 502');
  });

  it('should keep counting when one bulk endpoint fails and still error if under the limit', async () => {
    const nowISO = new Date().toISOString();
    const httpError: any = new Error('Unknown Marketo error');
    httpError.code = 503;

    protoStep.setData(Struct.fromJavaScript({ exportLimit: 500 }));

    clientWrapperStub.getBulkExportLeadJobs.returns(Promise.resolve({
      success: true,
      result: [{ finishedAt: nowISO, status: 'Completed', fileSize: 50 * 1024 * 1024 }],
    }));
    clientWrapperStub.getBulkExportActivityJobs.rejects(httpError);
    clientWrapperStub.getBulkExportProgramMemberJobs.returns(Promise.resolve({ success: true, result: [] }));
    clientWrapperStub.getCustomObjectTypes.returns(Promise.resolve({ success: true, result: [] }));

    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.ERROR);
    expect(response.getMessageFormat()).to.contain('incomplete');
    expect(response.getRecordsList()).to.have.lengthOf(0);
  });

  it('should fail on over-limit even when a later endpoint errors', async () => {
    const nowISO = new Date().toISOString();
    const httpError: any = new Error('Unknown Marketo error');
    httpError.code = 502;

    protoStep.setData(Struct.fromJavaScript({ exportLimit: 500 }));

    // 460MB from leads alone is already over 90% of 500MB
    clientWrapperStub.getBulkExportLeadJobs.returns(Promise.resolve({
      success: true,
      result: [{ finishedAt: nowISO, status: 'Completed', fileSize: 460 * 1024 * 1024 }],
    }));
    clientWrapperStub.getBulkExportActivityJobs.rejects(httpError);
    clientWrapperStub.getBulkExportProgramMemberJobs.returns(Promise.resolve({ success: true, result: [] }));
    clientWrapperStub.getCustomObjectTypes.returns(Promise.resolve({ success: true, result: [] }));

    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.FAILED);
    expect(response.getMessageFormat()).to.contain('Actual usage may be higher');
    expect(response.getRecordsList()).to.have.lengthOf(1);
  });

  it('should use default limit of 500MB when not specified', async () => {
    const nowISO = new Date().toISOString();

    // Don't set exportLimit, should default to 500MB
    protoStep.setData(Struct.fromJavaScript({}));

    clientWrapperStub.getBulkExportLeadJobs.returns(Promise.resolve({
      success: true,
      result: [
        {
          finishedAt: nowISO,
          status: 'Completed',
          fileSize: 400 * 1024 * 1024, // 400MB
        },
      ],
    }));
    
    clientWrapperStub.getBulkExportActivityJobs.returns(Promise.resolve({
      success: true,
      result: [],
    }));

    clientWrapperStub.getBulkExportProgramMemberJobs.returns(Promise.resolve({
      success: true,
      result: [],
    }));

    clientWrapperStub.getCustomObjectTypes.returns(Promise.resolve({ success: true, result: [] }));

    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    // Should pass because 400MB < 90% of 500MB (450MB)
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.PASSED);
  });

  it('should include custom object export jobs in usage calculation', async () => {
    const expectedLimit: number = 500;
    const nowISO = new Date().toISOString();

    protoStep.setData(Struct.fromJavaScript({
      exportLimit: expectedLimit,
    }));

    clientWrapperStub.getBulkExportLeadJobs.returns(Promise.resolve({ success: true, result: [] }));
    clientWrapperStub.getBulkExportActivityJobs.returns(Promise.resolve({ success: true, result: [] }));
    clientWrapperStub.getBulkExportProgramMemberJobs.returns(Promise.resolve({ success: true, result: [] }));

    // Two custom object types
    clientWrapperStub.getCustomObjectTypes.returns(Promise.resolve({
      success: true,
      result: [
        { name: 'cars_c' },
        { name: 'orders_c' },
      ],
    }));

    // cars_c: 200MB completed today
    // orders_c: 250MB completed today — total 450MB = 90% of 500MB limit, should fail
    clientWrapperStub.getBulkExportCustomObjectJobs.withArgs('cars_c').returns(Promise.resolve({
      success: true,
      result: [
        {
          finishedAt: nowISO,
          status: 'Completed',
          fileSize: 200 * 1024 * 1024,
        },
      ],
    }));
    clientWrapperStub.getBulkExportCustomObjectJobs.withArgs('orders_c').returns(Promise.resolve({
      success: true,
      result: [
        {
          finishedAt: nowISO,
          status: 'Completed',
          fileSize: 250 * 1024 * 1024,
        },
      ],
    }));

    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    // 450MB >= 90% of 500MB (450MB), should fail
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.FAILED);
  });

  it('should not count jobs whose finishedAt is before midnight Central Time', async () => {
    // Marketo resets at midnight Central Time. A job that finished at e.g. 1am UTC is
    // still the previous calendar day in Central Time (UTC-5/UTC-6) and must not count.
    // 48 hours ago is unambiguously a previous day in any timezone.
    const twoDaysAgoISO = new Date(Date.now() - 48 * 60 * 60 * 1000).toISOString();

    protoStep.setData(Struct.fromJavaScript({ exportLimit: 500 }));

    clientWrapperStub.getBulkExportLeadJobs.returns(Promise.resolve({
      success: true,
      result: [
        {
          finishedAt: twoDaysAgoISO,
          status: 'Completed',
          fileSize: 460 * 1024 * 1024, // 460MB — would fail if counted, but should not be
        },
      ],
    }));
    clientWrapperStub.getBulkExportActivityJobs.returns(Promise.resolve({ success: true, result: [] }));
    clientWrapperStub.getBulkExportProgramMemberJobs.returns(Promise.resolve({ success: true, result: [] }));
    clientWrapperStub.getCustomObjectTypes.returns(Promise.resolve({ success: true, result: [] }));

    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    // Should pass — the large job is from a previous day and must not count
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.PASSED);
  });

  it('should gracefully handle getCustomObjectTypes returning no result', async () => {
    protoStep.setData(Struct.fromJavaScript({ exportLimit: 500 }));

    clientWrapperStub.getBulkExportLeadJobs.returns(Promise.resolve({ success: true, result: [] }));
    clientWrapperStub.getBulkExportActivityJobs.returns(Promise.resolve({ success: true, result: [] }));
    clientWrapperStub.getBulkExportProgramMemberJobs.returns(Promise.resolve({ success: true, result: [] }));

    // Simulate API returning success:false with no result (e.g. no custom objects configured)
    clientWrapperStub.getCustomObjectTypes.returns(Promise.resolve({ success: false }));

    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.PASSED);
  });

  it('should add previousUsageMB to this user\'s usage when accumulating across multiple users', async () => {
    const nowISO = new Date().toISOString();

    // This user has 100MB, previous users accumulated 200MB → combined 300MB < 90% of 500MB
    protoStep.setData(Struct.fromJavaScript({
      exportLimit: 500,
      previousUsageMB: 200,
    }));

    clientWrapperStub.getBulkExportLeadJobs.returns(Promise.resolve({
      success: true,
      result: [{ finishedAt: nowISO, status: 'Completed', fileSize: 100 * 1024 * 1024 }],
    }));
    clientWrapperStub.getBulkExportActivityJobs.returns(Promise.resolve({ success: true, result: [] }));
    clientWrapperStub.getBulkExportProgramMemberJobs.returns(Promise.resolve({ success: true, result: [] }));
    clientWrapperStub.getCustomObjectTypes.returns(Promise.resolve({ success: true, result: [] }));

    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    // 300MB combined < 450MB (90% of 500MB) — should pass
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.PASSED);
  });

  it('should fail when combined usage with previousUsageMB exceeds 90% of the limit', async () => {
    const nowISO = new Date().toISOString();

    // This user has 100MB, previous users accumulated 360MB → combined 460MB > 90% of 500MB
    protoStep.setData(Struct.fromJavaScript({
      exportLimit: 500,
      previousUsageMB: 360,
    }));

    clientWrapperStub.getBulkExportLeadJobs.returns(Promise.resolve({
      success: true,
      result: [{ finishedAt: nowISO, status: 'Completed', fileSize: 100 * 1024 * 1024 }],
    }));
    clientWrapperStub.getBulkExportActivityJobs.returns(Promise.resolve({ success: true, result: [] }));
    clientWrapperStub.getBulkExportProgramMemberJobs.returns(Promise.resolve({ success: true, result: [] }));
    clientWrapperStub.getCustomObjectTypes.returns(Promise.resolve({ success: true, result: [] }));

    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    // 460MB combined >= 450MB (90% of 500MB) — should fail
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.FAILED);
  });

  it('should output combined total as bulkApiUsage so the next step can accumulate further', async () => {
    const nowISO = new Date().toISOString();

    // This user: 100MB. Previous: 150MB. Expected output token: 250MB
    protoStep.setData(Struct.fromJavaScript({
      exportLimit: 500,
      previousUsageMB: 150,
    }));

    clientWrapperStub.getBulkExportLeadJobs.returns(Promise.resolve({
      success: true,
      result: [{ finishedAt: nowISO, status: 'Completed', fileSize: 100 * 1024 * 1024 }],
    }));
    clientWrapperStub.getBulkExportActivityJobs.returns(Promise.resolve({ success: true, result: [] }));
    clientWrapperStub.getBulkExportProgramMemberJobs.returns(Promise.resolve({ success: true, result: [] }));
    clientWrapperStub.getCustomObjectTypes.returns(Promise.resolve({ success: true, result: [] }));

    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.PASSED);

    // The record should carry the combined 250MB so the next step's
    // {{marketo.bulkExports.bulkApiUsage}} token resolves to 250
    const record = response.getRecordsList()[0];
    const recordData = record.getKeyValue().toJavaScript();
    expect(recordData['bulkApiUsage']).to.equal(250);
  });
});
