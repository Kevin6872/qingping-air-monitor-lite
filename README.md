# Qingping Air Monitor Lite for Home Assistant

[![GPL-3.0](https://img.shields.io/github/license/Kevin6872/qingping-air-monitor-lite)](LICENSE)
[![HACS Custom](https://img.shields.io/badge/HACS-Custom-41BDF5.svg)](https://hacs.xyz/docs/faq/custom_repositories/)

适用于 Home Assistant 的非官方青萍空气检测仪 Lite（`CGDN1`）自定义集成。

它复用 Home Assistant 已建立的 **HomeKit Device** 连接读取本地传感器数据，并把额外的设备设置作为配置实体挂接到同一个 HomeKit 设备中。不会重复创建 HomeKit 已提供的温度、湿度、CO₂、PM2.5、PM10 和电池传感器。

> [!IMPORTANT]
> 设置控制依赖青萍云端私有 API 和厂商 MQTT WebSocket，不是本地 MQTT。传感器读取仍由 HomeKit 在局域网内完成。

## 功能

- 复用现有 HomeKit 配对，不接管或重复配对设备。
- 所有附加实体关联到已有 HomeKit 设备，不创建第二个设备。
- 读取 HomeKit 连接状态、固件版本、MAC 地址和充电状态。
- 调整传感器校准、屏幕显示、时区、时钟格式、夜间模式和 CO₂ 自动校准。
- 每次写入前读取设备的最新完整设置，避免旧缓存覆盖青萍 App 中刚修改的其他选项。
- 写入按请求 `cid` 对应回复，并在设备不回显 `cid` 时使用严格的内容匹配。
- 写入后再次读取设备设置进行确认。
- MQTT 凭证失效或连接中断后自动重新获取凭证。
- 支持英文和简体中文界面。

## 支持情况

| 项目 | 支持情况 |
| --- | --- |
| 设备型号 | 青萍空气检测仪 Lite `CGDN1` |
| Home Assistant | 已在 `2026.7.4` 验证，并适配 `2026.8` 单配置条目设备模型 |
| 本地传感器读取 | HomeKit Device |
| 设置读取与控制 | 青萍云端 API + MQTT WebSocket |
| 本地 MQTT | 不支持 |
| 配置方式 | Home Assistant UI Config Flow |

## 安装

### HACS

1. 在 HACS 中打开“自定义存储库”。
2. 填入 `https://github.com/Kevin6872/qingping-air-monitor-lite`，类别选择“集成”。
3. 安装 **Qingping Air Monitor Lite**。
4. 重启 Home Assistant。

### 手动安装

1. 将 `custom_components/qingping_air_monitor_lite` 复制到 Home Assistant 配置目录下的 `custom_components`。
2. 确认最终路径为：

   ```text
   /config/custom_components/qingping_air_monitor_lite
   ```

3. 重启 Home Assistant。

## 前置条件

1. 青萍空气检测仪 Lite 已通过 Home Assistant 的 **HomeKit Device** 集成完成配对。
2. Home Assistant 可以访问互联网，以及青萍 API 和下发的 MQTT WebSocket 地址。
3. 设备当前处于在线状态。
4. 你已从自己合法取得的青萍 App 安装中获得接口签名密钥。本仓库不附带厂商密钥。

## 配置

进入“设置 → 设备与服务 → 添加集成”，搜索 **Qingping Air Monitor Lite**。

| 字段 | 说明 |
| --- | --- |
| 名称 | 集成显示名称 |
| MAC 地址 | 设备的 12 位十六进制 MAC；支持带冒号或连字符的写法 |
| HomeKit 配对 ID | 可选；通常可根据型号和名称自动找到，存在多个同型号设备时建议填写 |
| 青萍 App 签名密钥 | 必填；以密码字段输入并存储在 HA 配置条目中，不包含在本项目源码内 |

配置完成后，新增实体会出现在原有 HomeKit 设备中。

## 实体

### 诊断传感器

| 实体 | 数据来源 |
| --- | --- |
| 状态 | HomeKit 连接 |
| 固件 | HomeKit |
| MAC 地址 | 配置 |
| 充电状态 | HomeKit |
| 云端连接 | 青萍 MQTT |

### 数值设置

| 设置 | 范围 |
| --- | --- |
| 温度偏移 | -10–10 °C，步进 0.1 |
| 湿度偏移 | -20–20%，步进 0.1 |
| CO₂ 偏移 | -500–500 ppm |
| PM2.5 偏移 | -500–500 µg/m³ |
| PM10 偏移 | -500–500 µg/m³ |
| 自动关机时间 | 0–60 分钟 |
| 熄屏时间 | 60–600 秒 |
| 屏保启动延迟 | 60–600 秒 |
| 时区 | UTC -12 至 UTC +14 |

### 选项与开关

| 设置 | 选项 |
| --- | --- |
| 屏幕模式 | 轮播读数、当前读数、时间 + 轮播读数、时间 + 当前读数 |
| 显示温度单位 | 摄氏度、华氏度 |
| 时钟格式 | 24 小时、12 小时 |
| CO₂ 自动校准 | 开、关 |
| 夜间模式开始/结束 | 时间选择器 |

## 数据与隐私

- HomeKit 传感器读数在局域网内由 Home Assistant 获取。
- 为建立控制通道，集成会向青萍云端提交设备 MAC、型号、HomeKit 配对标识、序列号以及软硬件版本。
- 集成会创建并保存青萍云端访问令牌和用户 ID；这些数据保存在 Home Assistant 配置条目中，不会作为实体状态公开。
- 青萍 App 签名密钥同样保存在配置条目中，配置与重新配置表单不会预填或返回现有值。
- MQTT 用户名和密码仅用于运行时连接，不写入日志或实体属性。
- 本项目不包含个人设备标识、Home Assistant 令牌或局域网地址。

## 已知限制

- 这是非官方集成，依赖从青萍 App 行为分析得到的私有协议；厂商更新 API、签名方式或 MQTT 格式后可能失效。
- 设置控制必须经过青萍云端，断网时仅 HomeKit 传感器部分可继续工作。
- 目前只验证了 `CGDN1`，不要用于其他青萍型号。
- 如果多个 HomeKit 设备具有相同型号且名称无法区分，需要手动填写 HomeKit 配对 ID。

## 故障排查

### 找不到 HomeKit 设备

- 确认设备已由 **HomeKit Device** 集成添加并在线。
- 检查型号是否为 `CGDN1`。
- 在配置时填写准确的 HomeKit 配对 ID。

### 云端连接显示离线

- 检查 Home Assistant 的互联网连接。
- 在设备与服务中选择本集成的“重新配置”，确认青萍 App 签名密钥有效。
- 等待自动刷新 MQTT 凭证并重连。
- 查看 Home Assistant 日志中 `qingping_air_monitor_lite` 的记录。

### 设置提交失败

- 确认“云端连接”实体为 `online`。
- 集成只有在收到设备确认并完成读回后才会把调用视为成功；超时或值不一致会返回错误。

## 免责声明

本项目与青萍科技、ClearGrass 或 Home Assistant 官方无关。使用私有云端接口可能受到厂商服务条款约束，请自行评估后使用。

## 许可证

本项目采用 [GNU General Public License v3.0](LICENSE)。
