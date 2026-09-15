# 签到数码管 OCR：ssocr 部署说明

签到图片中的 HH:MM:SS 数码管优先由外部 `ssocr` 程序识别。机器人通过系统 PATH 查找 `ssocr` 或 `ssocr.exe`，不新增配置项，也不会在启动时下载或编译该程序。

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

最小诊断：

```bash
ssocr -V
python -c "import shutil; print(shutil.which('ssocr') or shutil.which('ssocr.exe'))"
python -m cooper_bot.modules.vision.signin_ocr <一张真实签到图片>
```

第二条输出 `None` 时，请修正系统 PATH。在 Docker 部署中，应把 ssocr 安装到实际运行 Cooper_bot Python 的镜像或宿主机，而不是仅安装到 NapCat 容器。

## 缺失时的行为

- ssocr 可用时，签到数码管识别优先使用 ssocr。
- ssocr 不可用、超时或识别失败时，继续尝试现有 RapidOCR fallback。
- 两者均失败时，保持原有“未识别到有效时间”的结果；不会阻止机器人启动。

`ssocr` 不是 Python 包，因此无需修改 `requirements.txt`。不要将 ssocr 源码或二进制提交到本仓库。

## 第三方项目与许可证

Seven Segment Optical Character Recognition (ssocr)  
Project: <https://github.com/auerswal/ssocr>  
License: GPL-3.0-or-later

Cooper_bot 仅通过系统中的外部进程调用 ssocr，不包含其源码或二进制。
