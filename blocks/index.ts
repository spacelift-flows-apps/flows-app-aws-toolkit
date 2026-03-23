import { subscribeEventBridgeRule } from "./subscribeEventBridgeRule";
import { subscribeSNSTopic } from "./subscribeSNSTopic";

export const blocks = {
  subscribeEventBridgeRule,
  subscribeSNSTopic,
} as const;
