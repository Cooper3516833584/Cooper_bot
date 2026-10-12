# 签到数码管 OCR：ssocr 部署说明

签到图片中的 HH:MM:SS 数码管优先由外部 `ssocr` 程序识别。机器人优先通过系统 PATH 查找 `ssocr` 或 `ssocr.exe`；Windows 缺少原生程序时，自动探测默认 WSL 发行版中的 ssocr。不新增配置项，也不会自动下载或编译程序。

## 安装与验证

Debian/Ubuntu：

```bash
sudo apt-get update
sudo apt-get install -y ssocr
ssocr -V
command -v ssocr
```

如果发行版没有该包，请按 [ssocr INSTALL](https://github.com/auerswal/ssocr/blob/master/INSTALL) 从源码编译。Debian/Ubuntu 常见构建依赖为：

```bash
sudo apt-get install -y build-essential pkg-config libimlib2-dev libx11-dev
```

Windows 可通过 Cygwin 安装或编译 ssocr；需保证运行 Cooper_bot 的同一 Windows 用户能够从 PATH 找到 `ssocr.exe` 及其依赖。macOS 需先安装 Imlib2，再按上游 INSTALL 编译。

本机实际使用 Windows Python → WSL Ubuntu-24.04 → ssocr。在默认 WSL Ubuntu 中安装 `ssocr` 和 `coreutils`（提供 `timeout`）即可，无需原生 EXE。WSL 发行版属于 Windows 用户；请使用运行机器人的同一账户安装，并确保默认发行版正确。探测不会扫描其他发行版或修改默认发行版。

最小诊断：

```bash
ssocr -V
python -c "import shutil; print(shutil.which('ssocr') or shutil.which('ssocr.exe'))"
python -m cooper_bot.modules.vision.signin_ocr <一张真实签到图片>
```

第二条只检查原生程序。完整自动发现结果可用 `python -c "from cooper_bot.modules.vision.signin_ocr import _find_ssocr_executable; print(_find_ssocr_executable())"` 检查。包含 `wsl.exe` 的元组表示已接入 WSL；结果仍为 `None` 才表示两种方式均不可用。在 Docker 部署中，应把 ssocr 安装到实际运行 Cooper_bot Python 的镜像或宿主机，而不是仅安装到 NapCat 容器。

## 缺失时的行为

- ssocr 可用时，签到数码管识别优先使用 ssocr。
- ssocr 不可用、超时或识别失败时，继续尝试现有 RapidOCR fallback。
- 同一面板至少两种阈值结果一致才接受。秒级结果不足时补充高阈值重试，以分离模糊粘连的笔画。时、分读数冲突默认拒绝；唯一例外是同一面板的完整读数中，只有最低阈值异常，其余至少三个不同的更高阈值全部一致，且所有读数秒位相同，此时排除低阈值光晕或冒号残留造成的异常读数。重复阈值不计作独立支持，其他面板的冲突仍会拒识；同一面板只有秒位冲突时可降为分钟精度。
- ssocr 六位分割成功但秒位是未知字符或不合法数字时，只保留合法的前四位。必须至少两种阈值确认同一时、分；小时或分钟未知、少于六位、分割失败、跨分钟切换等情况不能回退。单次读数或时分冲突不再用 RapidOCR 覆盖。
- 分钟级结果明确返回 `HH:MM`，reason 包含 `minute_consensus`，不会伪造 `:00`。签到时间戳校验按该分钟的 00～59 秒区间计算距离，保持 3 分钟容差；与时间戳求平均时用分钟中点 `:30` 作为估计，保持既有 30 分钟截止时间容差和消息截止规则。
- 两者均失败时，保持原有“未识别到有效时间”的结果；不会阻止机器人启动。

`ssocr` 不是 Python 包，因此无需修改 `requirements.txt`。不要将 ssocr 源码或二进制提交到本仓库。

## 第三方项目与许可证

Seven Segment Optical Character Recognition (ssocr)  
Project: <https://github.com/auerswal/ssocr>  
License: GPL-3.0-or-later

Cooper_bot 仅通过系统中的外部进程调用 ssocr，不包含其源码或二进制。

## 候选定位与回归验证

当前流程先找带红色发光区域的完整黑色面板，再校正面板旋转，使用红通道自适应阈值保留亮芯和较暗笔画。随后校正笔画倾斜、按厚度抑制光晕、移除成对冒号点，交给 ssocr 识别六位数字。最多处理 4 个面板、每个先尝试 3 种阈值，未达成一致时最多追加 2 种，原生进程超时 2 秒；WSL 内部用 `timeout 2s` 限制识别进程，Windows 侧等待上限 3 秒。首次 WSL 探测允许 8 秒冷启动，探测结果（含失败）缓存 60 秒，并加锁避免并发重复启动。调用隐藏窗口、使用参数列表和 PNG 标准输入，不执行 shell、不共享图片路径。签到主流程仍通过 `asyncio.to_thread` 运行识别。置信度是保守的规则分数，不代表经统计校准的准确率。

在原图旁重新生成 `*.signin_debug.png`，同时生成 `signin_ocr_contact.png` 总览和逐图读数报告：

```bash
python -m tools.diagnostics.render_signin_ocr test/ocr
```

默认只读 `*.jpg`；其他格式可传 `--pattern "*.png"`。可用 `--labels test/ocr/labels.json` 对照人工标注，标注格式为文件名到 `HH:MM:SS` 的 JSON 映射。标注只用于评估，不参与识别。测试图片、渲染图和报告均位于已忽略的 `test/ocr/`。

预览文件被查看器占用而无法覆盖时，可用 `--output-dir test/ocr/latest` 将本次预览和报告写入单独目录。

Windows 默认自动发现 WSL 后端，无需额外参数；也可显式指定 WSL 进行独立诊断：

```powershell
python -m tools.diagnostics.render_signin_ocr test/ocr --wsl-ssocr
$env:SIGNIN_OCR_TEST_WSL = '1'
python -m pytest test/unit/test_signin_ocr.py
```

这两个开关只用于诊断和测试，不是上线开关。机器人和不带参数的测试都使用同一自动发现逻辑：原生 ssocr 优先，Windows 再尝试 WSL，均不可用时回退 RapidOCR。没有真实 ssocr 时，外部程序集成测试会明确跳过，其余测试仍运行。

实拍验证来自少量教室场景；全对不等于任意场景都准确。严重遮挡、时分缺段、极端透视、非红色或没有完整暗色边框的时钟仍需额外样本验证；不使用文件名或 EXIF 时间填补视觉识别失败。读分钟的能力只容忍秒位异常，无法恢复图片里已经丢失的时分信息。
