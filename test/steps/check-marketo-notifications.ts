import { Struct } from 'google-protobuf/google/protobuf/struct_pb';
import * as chai from 'chai';
import { default as sinon } from 'ts-sinon';
import * as sinonChai from 'sinon-chai';
import 'mocha';

import { Step as ProtoStep, StepDefinition, RunStepResponse } from '../../src/proto/cog_pb';
import { Step } from '../../src/steps/check-marketo-notifications';

chai.use(sinonChai);

describe('CheckMarketoNotificationsStep', () => {
  const expect = chai.expect;
  let protoStep: ProtoStep;
  let stepUnderTest: Step;
  let clientWrapperStub: any;

  beforeEach(() => {
    protoStep = new ProtoStep();
    clientWrapperStub = sinon.stub();
    clientWrapperStub.getNotificationInbox = sinon.stub();
    clientWrapperStub.getNotificationMessages = sinon.stub();
    stepUnderTest = new Step(clientWrapperStub);
  });

  it('should return expected step metadata', () => {
    const stepDef: StepDefinition = stepUnderTest.getDefinition();
    expect(stepDef.getStepId()).to.equal('CheckMarketoNotificationsStep');
    expect(stepDef.getName()).to.equal('Check Marketo notifications');
    expect(stepDef.getExpression()).to.equal('there should be no marketo notification matching (?<regex>.+)');
    expect(stepDef.getType()).to.equal(StepDefinition.Type.VALIDATION);
    expect(stepDef.getTargetObject()).to.equal('Marketo Notifications');
  });

  it('should fail when a notification matches the regex', async () => {
    clientWrapperStub.getNotificationInbox.returns('abc@thisisjust.atomatest.com');
    clientWrapperStub.getNotificationMessages.resolves([
      { subject: 'Salesforce Sync Error', from: 'Marketo', receivedAt: '2026-08-30T00:00:00.000Z', body: '' },
    ]);
    protoStep.setData(Struct.fromJavaScript({ regex: 'sync error' }));

    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.FAILED);
  });

  it('should pass when no notification matches', async () => {
    clientWrapperStub.getNotificationInbox.returns('abc@thisisjust.atomatest.com');
    clientWrapperStub.getNotificationMessages.resolves([
      { subject: 'Campaign Failure', from: 'Marketo', receivedAt: '2026-08-30T00:00:00.000Z', body: '' },
    ]);
    protoStep.setData(Struct.fromJavaScript({ regex: 'sync error' }));

    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.PASSED);
  });

  it('should error on an invalid regex', async () => {
    clientWrapperStub.getNotificationInbox.returns('abc@thisisjust.atomatest.com');
    protoStep.setData(Struct.fromJavaScript({ regex: '(' }));

    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.ERROR);
  });

  it('should error when the inbox is missing', async () => {
    clientWrapperStub.getNotificationInbox.returns('');
    protoStep.setData(Struct.fromJavaScript({ regex: 'sync' }));

    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.ERROR);
  });

  it('should error when Mailgun throws', async () => {
    clientWrapperStub.getNotificationInbox.returns('abc@thisisjust.atomatest.com');
    clientWrapperStub.getNotificationMessages.rejects(new Error('mailgun down'));
    protoStep.setData(Struct.fromJavaScript({ regex: 'sync' }));

    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.ERROR);
  });
});
