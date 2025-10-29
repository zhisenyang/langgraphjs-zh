import { ChatOpenAI } from "@langchain/openai";
import {
  SystemMessage,
  HumanMessage,
  RemoveMessage,
} from "@langchain/core/messages";
import { MemorySaver } from "@langchain/langgraph-checkpoint";
import {
  MessagesAnnotation,
  StateGraph,
  START,
  END,
  Annotation,
} from "@langchain/langgraph";
import { v4 as uuidv4 } from "uuid";

const memory = new MemorySaver();

// 在对话状态上新增 summary 字段，用于记录历史摘要
const GraphAnnotation = Annotation.Root({
  ...MessagesAnnotation.spec,
  summary: Annotation<string>({
    reducer: (_, action) => action,
    default: () => "",
  }),
});

const model = new ChatOpenAI({
  model: "moonshotai/Kimi-K2-Instruct-0905",
  // apiKey: 'sk-fklomryoqfwwahhdjtadyndyuuqvisykjmhdcsprvqgyyusk',
  configuration: {
    apiKey: "zai-org/GLM-4.6",
    baseURL: "https://api.siliconflow.cn/v1",
    // logLevel: "debug",
    // logger: _debugLogger,
  },
});

async function callModel(
  state: typeof GraphAnnotation.State
): Promise<Partial<typeof GraphAnnotation.State>> {
  const { summary } = state;
  let { messages } = state;
  if (summary) {
    const systemMessage = new SystemMessage({
      id: uuidv4(),
      content: `Summary of conversation earlier: ${summary}`,
    });
    messages = [systemMessage, ...messages];
  }
  const response = await model.invoke(messages);
  return { messages: [response] };
}

function shouldContinue(
  state: typeof GraphAnnotation.State
): "summarize_conversation" | typeof END {
  const { messages } = state;
  if (messages.length > 6) {
    return "summarize_conversation";
  }
  return END;
}

async function summarizeConversation(
  state: typeof GraphAnnotation.State
): Promise<Partial<typeof GraphAnnotation.State>> {
  const { summary, messages } = state;
  const summaryPrompt = summary
    ? `This is summary of the conversation to date: ${summary}\n\nExtend the summary by taking into account the new messages above:`
    : "Create a summary of the conversation above:";

  const allMessages = [
    ...messages,
    new HumanMessage({
      id: uuidv4(),
      content: summaryPrompt,
    }),
  ];
  const response = await model.invoke(allMessages);
  const deleteMessages = messages
    .slice(0, -2)
    .map((m) => new RemoveMessage({ id: m.id }));
  if (typeof response.content !== "string") {
    throw new Error("Expected a string response from the model");
  }
  return { summary: response.content, messages: deleteMessages };
}

const workflow = new StateGraph(GraphAnnotation)
  .addNode("conversation", callModel)
  .addNode("summarize_conversation", summarizeConversation)
  .addEdge(START, "conversation")
  .addConditionalEdges("conversation", shouldContinue)
  .addEdge("summarize_conversation", END);

const app = workflow.compile({ checkpointer: memory });

const printUpdate = (update: Record<string, any>) => {
  Object.keys(update).forEach((key) => {
    const value = update[key];
    if ("messages" in value && Array.isArray(value.messages)) {
      value.messages.forEach((msg) => {
        console.log(
          `\n================================ ${msg._getType()} Message =================================`
        );
        console.log(msg.content);
      });
    }
    if ("summary" in value && value.summary) {
      console.log(value.summary);
    }
  });
};

const config = {
  configurable: { thread_id: "4" },
  streamMode: "updates" as const,
};

async function streamAndPrint(content: string) {
  const message = new HumanMessage(content);
  console.log(message.content);
  for await (const event of await app.stream({ messages: [message] }, config)) {
    printUpdate(event);
  }
}

async function logState(label: string) {
  const { values } = await app.getState(config);
  console.log(`\n>>> ${label}`);
  console.dir(values, { depth: null });
}

async function main() {
  await streamAndPrint("hi! I'm bob");
  await streamAndPrint("What did I sat my name was?");
  await streamAndPrint("i like the celtics!");

  await logState("state after three turns");

  await streamAndPrint("i like how much they win");

  await logState("state after fourth turn");

  await streamAndPrint("what's my name?");
  await streamAndPrint("what NFL team do you think I like?");
  await streamAndPrint("i like the patriots!");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
