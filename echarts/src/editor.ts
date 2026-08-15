/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// The editor pane: CodeJar (~2 KB, a contenteditable with indentation and undo) highlighted by
// Prism, and the evaluator that turns what is typed into an ECharts option.
//
// The source is JavaScript, not JSON, because half of what makes an ECharts option worth editing --
// formatters, `z` callbacks, generated data -- is functions, and because it is what every ECharts
// example is written in.  It is evaluated as an expression in the visitor's own tab with two names in
// scope, so it can only reach what the page itself can.

import type { EChartsCoreOption } from "echarts/core";
import { CodeJar } from "codejar";
import Prism from "prismjs";

Prism.manual = true;

export interface Editor {
  value(): string;
  set(code: string): void;
  onChange(handler: (code: string) => void): void;
}

export function createEditor(host: HTMLElement): Editor {
  const jar = CodeJar(
    host,
    (element) => {
      element.innerHTML = Prism.highlight(element.textContent ?? "", Prism.languages.javascript, "javascript");
    },
    // CodeJar's auto-closing inserts a bracket but does not step over the one you then type, which
    // turns typing an option out by hand into a mess of stray closers -- indent only.
    { tab: "  ", indentOn: /[([{]$/, catchTab: true, addClosing: false },
  );

  return {
    value: () => jar.toString(),
    set: (code) => jar.updateCode(code),
    onChange: (handler) => jar.onUpdate(handler),
  };
}

/**
 * A seeded generator (mulberry32), handed to the editor so presets can make their own data and still
 * draw the same chart on every reload.
 */
export function random(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Options as they come out of the editor: one, or a sequence of frames to step through. */
export type Frames = EChartsCoreOption[];

/** Evaluate the editor's source into frames, or throw with a message worth showing. */
export function evaluate(source: string): Frames {
  const trimmed = source.trim();
  if (!trimmed) throw new Error("the editor is empty");

  let value: unknown;
  try {
    // Parenthesised so a bare `{ ... }` is an object literal rather than a block, which is how every
    // ECharts option in the wild is written.
    value = new Function("random", `"use strict";\nreturn (\n${trimmed}\n);`)(random);
  } catch (error) {
    throw new Error(`${(error as Error).name}: ${(error as Error).message}`);
  }

  const frames = Array.isArray(value) ? value : [value];
  if (!frames.length) throw new Error("an empty array is not an option");
  for (const frame of frames) {
    if (!frame || typeof frame !== "object") {
      throw new Error("the source must evaluate to an option object, or an array of them");
    }
  }
  return frames as Frames;
}
