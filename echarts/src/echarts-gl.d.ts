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

// echarts-gl ships no type declarations.  Its tree-shakeable entry points expose what every ECharts
// extension does -- opaque installers handed to `echarts.use()` -- and ECharts does not export that
// type, so it is recovered from `use`'s own signature.

type EChartsInstaller = Extract<Parameters<typeof import("echarts/core").use>[0], (arg: never) => unknown>;

declare module "echarts-gl/charts" {
  export const Bar3DChart: EChartsInstaller;
  export const Line3DChart: EChartsInstaller;
  export const Scatter3DChart: EChartsInstaller;
  export const SurfaceChart: EChartsInstaller;
}

declare module "echarts-gl/components" {
  export const Grid3DComponent: EChartsInstaller;
}
