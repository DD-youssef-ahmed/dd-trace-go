// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/dd-trace-go/v2/datastreams"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/mocktracer"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
)

func TestEnrichOperation(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		input     middleware.InitializeInput
		setup     func(context.Context) *tracer.Span
		check     func(*testing.T, middleware.InitializeInput, *tracer.Span)
	}{
		{
			name:      "SendMessage",
			operation: "SendMessage",
			input: middleware.InitializeInput{
				Parameters: &sqs.SendMessageInput{
					MessageBody: aws.String("test message"),
					QueueUrl:    aws.String("https://sqs.us-east-1.amazonaws.com/1234567890/test-queue"),
				},
			},
			setup: func(ctx context.Context) *tracer.Span {
				span, _ := tracer.StartSpanFromContext(ctx, "test-span")
				return span
			},
			check: func(t *testing.T, in middleware.InitializeInput, span *tracer.Span) {
				params, ok := in.Parameters.(*sqs.SendMessageInput)
				require.True(t, ok)
				require.NotNil(t, params)
				require.NotNil(t, params.MessageAttributes)
				assert.Contains(t, params.MessageAttributes, datadogKey)
				assert.NotNil(t, params.MessageAttributes[datadogKey].DataType)
				assert.Equal(t, "String", *params.MessageAttributes[datadogKey].DataType)
				assert.NotNil(t, params.MessageAttributes[datadogKey].StringValue)
				assert.NotEmpty(t, *params.MessageAttributes[datadogKey].StringValue)
				require.Equal(t, span.AsMap()["messaging.system"], "amazonsqs")
			},
		},
		{
			name:      "SendMessageBatch",
			operation: "SendMessageBatch",
			input: middleware.InitializeInput{
				Parameters: &sqs.SendMessageBatchInput{
					QueueUrl: aws.String("https://sqs.us-east-1.amazonaws.com/1234567890/test-queue"),
					Entries: []types.SendMessageBatchRequestEntry{
						{
							Id:          aws.String("1"),
							MessageBody: aws.String("test message 1"),
						},
						{
							Id:          aws.String("2"),
							MessageBody: aws.String("test message 2"),
						},
						{
							Id:          aws.String("3"),
							MessageBody: aws.String("test message 3"),
						},
					},
				},
			},
			setup: func(ctx context.Context) *tracer.Span {
				span, _ := tracer.StartSpanFromContext(ctx, "test-span")
				return span
			},
			check: func(t *testing.T, in middleware.InitializeInput, span *tracer.Span) {
				params, ok := in.Parameters.(*sqs.SendMessageBatchInput)
				require.True(t, ok)
				require.NotNil(t, params)
				require.NotNil(t, params.Entries)
				require.Len(t, params.Entries, 3)

				for _, entry := range params.Entries {
					require.NotNil(t, entry.MessageAttributes)
					assert.Contains(t, entry.MessageAttributes, datadogKey)
					assert.NotNil(t, entry.MessageAttributes[datadogKey].DataType)
					assert.Equal(t, "String", *entry.MessageAttributes[datadogKey].DataType)
					assert.NotNil(t, entry.MessageAttributes[datadogKey].StringValue)
					assert.NotEmpty(t, *entry.MessageAttributes[datadogKey].StringValue)
				}
				require.Equal(t, span.AsMap()["messaging.system"], "amazonsqs")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mt := mocktracer.Start()
			defer mt.Stop()

			ctx := context.Background()
			span := tt.setup(ctx)

			EnrichOperation(ctx, span, tt.input, tt.operation, false, "test-queue")

			if tt.check != nil {
				tt.check(t, tt.input, span)
			}
		})
	}
}

func TestInjectTraceContext(t *testing.T) {
	tests := []struct {
		name               string
		existingAttributes int
		expectInjection    bool
	}{
		{
			name:               "Inject with no existing attributes",
			existingAttributes: 0,
			expectInjection:    true,
		},
		{
			name:               "Inject with some existing attributes",
			existingAttributes: 5,
			expectInjection:    true,
		},
		{
			name:               "No injection when at max attributes",
			existingAttributes: maxMessageAttributes,
			expectInjection:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mt := mocktracer.Start()
			defer mt.Stop()

			span := tracer.StartSpan("test-span")
			carrier := tracer.TextMapCarrier{}
			require.NoError(t, tracer.Inject(span.Context(), carrier))
			traceContext, err := encodeCarrier(carrier)
			require.NoError(t, err)

			messageAttributes := make(map[string]types.MessageAttributeValue)
			for i := 0; i < tt.existingAttributes; i++ {
				messageAttributes[fmt.Sprintf("attr%d", i)] = types.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String("value"),
				}
			}

			injectTraceContext(traceContext, messageAttributes)

			if tt.expectInjection {
				assert.Contains(t, messageAttributes, datadogKey)
				assert.NotNil(t, messageAttributes[datadogKey].DataType)
				assert.Equal(t, "String", *messageAttributes[datadogKey].DataType)
				assert.NotNil(t, messageAttributes[datadogKey].StringValue)
				assert.NotEmpty(t, *messageAttributes[datadogKey].StringValue)

				carrier := tracer.TextMapCarrier{}
				err := json.Unmarshal([]byte(*messageAttributes[datadogKey].StringValue), &carrier)
				assert.NoError(t, err)

				extractedSpanContext, err := tracer.Extract(carrier)
				assert.NoError(t, err)
				assert.Equal(t, span.Context().TraceID(), extractedSpanContext.TraceID())
				assert.Equal(t, span.Context().SpanID(), extractedSpanContext.SpanID())
			} else {
				assert.NotContains(t, messageAttributes, datadogKey)
			}
		})
	}
}

// TestSendMessageDSMCheckpoint verifies that when DSM is enabled, a produce
// checkpoint is set and the resulting pathway is embedded in the outgoing
// message's _datadog attribute.
func TestSendMessageDSMCheckpoint(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	queueName := "test-queue"
	input := middleware.InitializeInput{
		Parameters: &sqs.SendMessageInput{
			MessageBody: aws.String("hello"),
			QueueUrl:    aws.String("https://sqs.us-east-1.amazonaws.com/1234567890/" + queueName),
		},
	}
	span, ctx := tracer.StartSpanFromContext(context.Background(), "test-span")

	EnrichOperation(ctx, span, input, "SendMessage", true, queueName)

	params := input.Parameters.(*sqs.SendMessageInput)
	require.Contains(t, params.MessageAttributes, datadogKey)

	carrier := tracer.TextMapCarrier{}
	require.NoError(t, json.Unmarshal([]byte(*params.MessageAttributes[datadogKey].StringValue), &carrier))

	got, ok := datastreams.PathwayFromContext(datastreams.ExtractFromBase64Carrier(context.Background(), carrier))
	require.True(t, ok, "expected DSM pathway in injected _datadog attribute")

	wantCtx, _ := tracer.SetDataStreamsCheckpoint(context.Background(), "direction:out", "topic:"+queueName, "type:sqs")
	want, _ := datastreams.PathwayFromContext(wantCtx)
	assert.NotZero(t, want.GetHash())
	assert.Equal(t, want.GetHash(), got.GetHash())
}

// TestSendMessageBatchDSMCheckpoint verifies that each batch entry receives
// its own DSM produce checkpoint and pathway in the outgoing _datadog
// message attribute.
func TestSendMessageBatchDSMCheckpoint(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	queueName := "batch-queue"
	input := middleware.InitializeInput{
		Parameters: &sqs.SendMessageBatchInput{
			QueueUrl: aws.String("https://sqs.us-east-1.amazonaws.com/1234567890/" + queueName),
			Entries: []types.SendMessageBatchRequestEntry{
				{Id: aws.String("1"), MessageBody: aws.String("m1")},
				{Id: aws.String("2"), MessageBody: aws.String("m2")},
			},
		},
	}
	span, ctx := tracer.StartSpanFromContext(context.Background(), "test-span")
	EnrichOperation(ctx, span, input, "SendMessageBatch", true, queueName)

	params := input.Parameters.(*sqs.SendMessageBatchInput)
	for i, entry := range params.Entries {
		require.Containsf(t, entry.MessageAttributes, datadogKey, "entry %d missing _datadog", i)
		carrier := tracer.TextMapCarrier{}
		require.NoError(t, json.Unmarshal([]byte(*entry.MessageAttributes[datadogKey].StringValue), &carrier))
		got, ok := datastreams.PathwayFromContext(datastreams.ExtractFromBase64Carrier(context.Background(), carrier))
		require.True(t, ok, "expected DSM pathway in entry %d", i)
		assert.NotZero(t, got.GetHash())
	}
}

// TestReceiveMessageRequestAddsDatadogAttribute verifies that when DSM is
// enabled, the ReceiveMessage input is mutated to request the _datadog
// message attribute so the SQS server returns it in the response.
func TestReceiveMessageRequestAddsDatadogAttribute(t *testing.T) {
	tests := []struct {
		name      string
		initial   []string
		wantNames []string
	}{
		{
			name:      "empty",
			initial:   nil,
			wantNames: []string{datadogKey},
		},
		{
			name:      "preserves existing",
			initial:   []string{"foo"},
			wantNames: []string{"foo", datadogKey},
		},
		{
			name:      "no-op when _datadog already present",
			initial:   []string{datadogKey},
			wantNames: []string{datadogKey},
		},
		{
			name:      "no-op when All present",
			initial:   []string{"All"},
			wantNames: []string{"All"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mt := mocktracer.Start()
			defer mt.Stop()

			input := middleware.InitializeInput{
				Parameters: &sqs.ReceiveMessageInput{
					QueueUrl:              aws.String("https://sqs.us-east-1.amazonaws.com/1234567890/q"),
					MessageAttributeNames: tt.initial,
				},
			}
			span, ctx := tracer.StartSpanFromContext(context.Background(), "test-span")
			EnrichOperation(ctx, span, input, "ReceiveMessage", true, "q")
			params := input.Parameters.(*sqs.ReceiveMessageInput)
			assert.Equal(t, tt.wantNames, params.MessageAttributeNames)
		})
	}
}

// TestReceiveMessageRequestNoDSMNoMutation ensures we do not mutate
// MessageAttributeNames when DSM is disabled.
func TestReceiveMessageRequestNoDSMNoMutation(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	input := middleware.InitializeInput{
		Parameters: &sqs.ReceiveMessageInput{
			QueueUrl: aws.String("https://sqs.us-east-1.amazonaws.com/1234567890/q"),
		},
	}
	span, ctx := tracer.StartSpanFromContext(context.Background(), "test-span")
	EnrichOperation(ctx, span, input, "ReceiveMessage", false, "q")
	params := input.Parameters.(*sqs.ReceiveMessageInput)
	assert.Empty(t, params.MessageAttributeNames)
}

// TestEnrichOperationOutputConsumeCheckpoint verifies that on a successful
// ReceiveMessage response, each message gets a DSM consume checkpoint whose
// pathway is chained from the producer's pathway (when present in the
// message attribute) and is written back to the message attribute.
func TestEnrichOperationOutputConsumeCheckpoint(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	queueName := "consume-queue"

	// Build a message whose _datadog attribute carries a producer pathway.
	producerCtx, ok := tracer.SetDataStreamsCheckpoint(context.Background(), "direction:out", "topic:"+queueName, "type:sqs")
	require.True(t, ok)
	producerCarrier := tracer.TextMapCarrier{}
	datastreams.InjectToBase64Carrier(producerCtx, producerCarrier)
	producerJSON, err := json.Marshal(producerCarrier)
	require.NoError(t, err)

	out := middleware.InitializeOutput{
		Result: &sqs.ReceiveMessageOutput{
			Messages: []types.Message{
				{
					Body: aws.String("payload"),
					MessageAttributes: map[string]types.MessageAttributeValue{
						datadogKey: {
							DataType:    aws.String("String"),
							StringValue: aws.String(string(producerJSON)),
						},
					},
				},
			},
		},
	}

	EnrichOperationOutput(out, "ReceiveMessage", true, queueName)

	resp := out.Result.(*sqs.ReceiveMessageOutput)
	require.Len(t, resp.Messages, 1)
	require.Contains(t, resp.Messages[0].MessageAttributes, datadogKey)

	consumeCarrier := tracer.TextMapCarrier{}
	require.NoError(t, json.Unmarshal([]byte(*resp.Messages[0].MessageAttributes[datadogKey].StringValue), &consumeCarrier))

	got, ok := datastreams.PathwayFromContext(datastreams.ExtractFromBase64Carrier(context.Background(), consumeCarrier))
	require.True(t, ok)

	wantCtx, _ := tracer.SetDataStreamsCheckpoint(producerCtx, "direction:in", "topic:"+queueName, "type:sqs")
	want, _ := datastreams.PathwayFromContext(wantCtx)
	assert.NotZero(t, want.GetHash())
	assert.Equal(t, want.GetHash(), got.GetHash())
}

// TestEnrichOperationOutputDisabled verifies that EnrichOperationOutput is a
// no-op when DSM is disabled.
func TestEnrichOperationOutputDisabled(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	out := middleware.InitializeOutput{
		Result: &sqs.ReceiveMessageOutput{
			Messages: []types.Message{
				{Body: aws.String("payload")},
			},
		},
	}
	EnrichOperationOutput(out, "ReceiveMessage", false, "q")
	resp := out.Result.(*sqs.ReceiveMessageOutput)
	assert.Nil(t, resp.Messages[0].MessageAttributes)
}
