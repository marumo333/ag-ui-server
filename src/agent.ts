import {
  AbstractAgent,
  EventType,
  BaseEvent,
  Message,
  AssistantMessage,
  RunAgentInput,
  TextMessageContentEvent,
  RunFinishedEvent,
  RunStartedEvent,
  ToolCallArgsEvent,
  ToolCallEndEvent,
  ToolCallStartEvent,
  ToolCall,
  ToolMessage,
  RunErrorEvent,
  TextMessageStartEvent,
  TextMessageEndEvent,
} from "@ag-ui/client";
import { Observable } from "rxjs";
import {
  ModelMessage,
  streamText,
  tool as createVercelAISDKTool,
  ToolSet,
} from "ai";
import { google } from "@ai-sdk/google";
import { randomUUID } from "crypto";
import { z } from "zod";

export class MyAgent extends AbstractAgent {
  constructor(config = {}) {
    super(config);
  }
  run(input: RunAgentInput): Observable<BaseEvent> {
    const finalMessages: Message[] = input.messages;

    // rxjs の Observable を使用して、非同期処理を行う
    return new Observable<BaseEvent>((subscriber) => {
      subscriber.next({
        // AG-UI で定義されているイベントタイプを使用して、イベントを発行する
        // RUN_STARTED イベントを発行して、エージェントの実行が開始されたことを通知する
        type: EventType.RUN_STARTED,
        threadId: input.threadId,
        runId: input.runId,
      } as RunStartedEvent);

      const response = streamText({
        // Google の Gemini 2.5 Flash Lite モデルを呼び出す
        // Vercel AI SDK は AI モデルを自由に切り替えられるように設計されている
        model: google("gemini-2.5-flash-lite"),
        // HTTP リクエストで受け取った受け取ったメッセージとツールを Vercel AI SDK の形式に変換してから設定する
        messages: convertMessagesToVercelAISDKMessages(input.messages),
        tools: convertToolToVerlAISDKTools(input.tools),
      });

      const messageId = randomUUID();
      // メッセージが開始されたことを通知する
      const startEvent: TextMessageStartEvent = {
        type: EventType.TEXT_MESSAGE_START,
        messageId,
        role: "assistant",
      };

      subscriber.next(startEvent);

      let assistantMessage: AssistantMessage = {
        id: messageId,
        role: "assistant",
        content: "",
        toolCalls: [],
      };
      finalMessages.push(assistantMessage);

      // fullStream を使ってストリーミングレスポンスを処理
      (async () => {
        try {
          for await (const part of response.fullStream) {
            if (part.type === 'text-delta') {
              assistantMessage.content += part.text;
              subscriber.next({
                type: EventType.TEXT_MESSAGE_CONTENT,
                messageId,
                delta: part.text,
              } as TextMessageContentEvent);
            } else if (part.type === 'text-end') {
              const event: TextMessageEndEvent = {
                type: EventType.TEXT_MESSAGE_END,
                messageId,
              };
              subscriber.next(event);
              // ストリームが完了したかどうかを確認するため、finish イベントを待つ
            } else if (part.type === 'tool-call') {
              let toolCall: ToolCall = {
                id: part.toolCallId,
                type: "function",
                function: {
                  name: part.toolName,
                  arguments: JSON.stringify(part.input),
                },
              };
              assistantMessage.toolCalls!.push(toolCall);

              const startEvent: ToolCallStartEvent = {
                type: EventType.TOOL_CALL_START,
                parentMessageId: messageId,
                toolCallId: part.toolCallId,
                toolCallName: part.toolName,
              };
              subscriber.next(startEvent);

              const argsEvent: ToolCallArgsEvent = {
                type: EventType.TOOL_CALL_ARGS,
                toolCallId: part.toolCallId,
                delta: JSON.stringify(part.input),
              };
              subscriber.next(argsEvent);

              const endEvent: ToolCallEndEvent = {
                type: EventType.TOOL_CALL_END,
                toolCallId: part.toolCallId,
              };
              subscriber.next(endEvent);
            } else if (part.type === 'tool-result') {
              const toolMessage: ToolMessage = {
                role: "tool",
                id: randomUUID(),
                toolCallId: part.toolCallId,
                content: JSON.stringify(part.output),
              };
              finalMessages.push(toolMessage);
            } else if (part.type === 'error') {
              const runErrorEvent: RunErrorEvent = {
                type: EventType.RUN_ERROR,
                message: part.error instanceof Error ? part.error.message : "An error occurred during the run",
              };
              subscriber.error(runErrorEvent);
              return;
            } else if (part.type === 'finish') {
              // ストリームが完了した場合
              const event: TextMessageEndEvent = {
                type: EventType.TEXT_MESSAGE_END,
                messageId,
              };
              subscriber.next(event);

              subscriber.next({
                type: EventType.RUN_FINISHED,
                threadId: input.threadId,
                runId: input.runId,
              } as RunFinishedEvent);

              subscriber.complete();
              return;
            }
          }
        } catch (error: any) {
          console.error("catch error", error);
          const runErrorEvent: RunErrorEvent = {
            type: EventType.RUN_ERROR,
            message: error.message,
            code: error.code,
          };
          subscriber.error(runErrorEvent);
        }
      })();

      return () => {};
    }) as any as Observable<BaseEvent>;
  }
}
function convertMessagesToVercelAISDKMessages(
  messages: Message[]
): ModelMessage[] {
  const result: ModelMessage[] = [];

  for (const message of messages) {
    if (message.role === "assistant") {
      const parts: any[] = message.content
        ? [{ type: "text", text: message.content }]
        : [];
      for (const toolCall of message.toolCalls ?? []) {
        parts.push({
          type: "tool-call",
          toolCallId: toolCall.id,
          toolName: toolCall.function.name,
          args: JSON.parse(toolCall.function.arguments),
        });
      }
      result.push({
        role: "assistant",
        content: parts,
      });
    } else if (message.role === "user") {
      if (typeof message.content === "string" || !message.content) {
        result.push({
          role: "user",
          content: message.content || "",
        });
      } else if (Array.isArray(message.content)) {
        // Assume each part is either { type: "text", text: string } or { type: "binary", ... }
        const converted = message.content.map((part: any) => {
          if (part.type === "text") {
            return {
              type: "text",
              text: part.text,
            };
          } else if (part.type === "binary") {
            // Map 'binary' to 'file', as expected by Vercel AI SDK (may need adjustment per actual types)
            return {
              type: "file",
              mediaType: part.mimeType,
              url: part.url,
              id: part.id,
              data: part.data,
              name: part.filename,
            };
          } else {
            return part;
          }
        });
        result.push({
          role: "user",
          content: converted,
        });
      }
    } else if (message.role === "tool") {
      let toolName = "unknown";
      for (const msg of messages) {
        if (msg.role === "assistant") {
          for (const toolCall of msg.toolCalls ?? []) {
            if (toolCall.id === message.toolCallId) {
              toolName = toolCall.function.name;
              break;
            }
          }
        }
      }
      // message.content を ToolResultOutput 形式に変換
      let output: any;
      if (typeof message.content === "string") {
        // JSON として解析を試みる
        try {
          const parsed = JSON.parse(message.content);
          output = { type: "json" as const, value: parsed };
        } catch {
          // JSON でない場合はテキストとして扱う
          output = { type: "text" as const, value: message.content };
        }
      } else {
        // 既にオブジェクトの場合は JSON として扱う
        output = { type: "json" as const, value: message.content };
      }

      result.push({
        role: "tool",
        content: [
          {
            type: "tool-result",
            toolCallId: message.toolCallId,
            toolName: toolName,
            output: output,
          },
        ],
      });
    }
  }

  return result;
}

function convertJsonSchemaToZodSchema(
  jsonSchema: any,
  required: boolean
): z.ZodSchema {
  if (jsonSchema.type === "object") {
    const spec: { [key: string]: z.ZodSchema } = {};

    if (!jsonSchema.properties || !Object.keys(jsonSchema.properties).length) {
      return !required ? z.object(spec).optional() : z.object(spec);
    }

    for (const [key, value] of Object.entries(jsonSchema.properties)) {
      spec[key] = convertJsonSchemaToZodSchema(
        value,
        jsonSchema.required ? jsonSchema.required.includes(key) : false
      );
    }
    let schema = z.object(spec).describe(jsonSchema.description);
    return required ? schema : schema.optional();
  } else if (jsonSchema.type === "string") {
    let schema = z.string().describe(jsonSchema.description);
    return required ? schema : schema.optional();
  } else if (jsonSchema.type === "number") {
    let schema = z.number().describe(jsonSchema.description);
    return required ? schema : schema.optional();
  } else if (jsonSchema.type === "boolean") {
    let schema = z.boolean().describe(jsonSchema.description);
    return required ? schema : schema.optional();
  } else if (jsonSchema.type === "array") {
    let itemSchema = convertJsonSchemaToZodSchema(jsonSchema.items, true);
    let schema = z.array(itemSchema).describe(jsonSchema.description);
    return required ? schema : schema.optional();
  }
  throw new Error("Invalid JSON schema");
}

function convertToolToVerlAISDKTools(tools: RunAgentInput["tools"]): ToolSet {
  return tools.reduce(
    (acc: ToolSet, tool: RunAgentInput["tools"][number]) => ({
      ...acc,
      [tool.name]: createVercelAISDKTool({
        description: tool.description,
        inputSchema: convertJsonSchemaToZodSchema(tool.parameters, true),
      }),
    }),
    {}
  );
}
