; 创懿基金量化投资平台 —— 前端补丁安装包（Inno Setup 6）
; 用途：只改前端（Vue 文案/样式/交互）时，不跑 Nuitka、不重打 200MB 全量包，
;       只需几 MB 的补丁包覆盖安装目录下的 _internal\web\dist。
; 前置：web\ 下 npm run build；main\.venv\Scripts\python.exe main\build_package.py --web-only
; 编译："C:\Users\<用户名>\AppData\Local\Programs\Inno Setup 6\ISCC.exe" packaging\installer_web.iss
;
; 与 installer.iss 共用同一个 AppId，因此覆盖安装同一「程序和功能」条目；
; 版本号沿用当前已发布版本（#include app-version.iss），不单独升版本——
; 版本号只在整体出包（installer.iss）时 bump。

#define AppName "创懿基金量化投资平台"
#include "app-version.iss"
#define BuildStamp GetDateTimeString('yyyymmdd-hhnnss', '', '')
#define AppPublisher "创懿私募基金"
#define AppExe "tq-iquant.exe"
#define DistDir "..\dist\tq-iquant"

[Setup]
AppId={{3B0F4A52-9C61-4C7A-8E1D-2F5B7A9C0D31}
AppName={#AppName}
AppVersion={#AppVersion}
AppVerName={#AppName} {#AppVersion}（前端更新包）
AppPublisher={#AppPublisher}
DefaultDirName=D:\{#AppName}
DefaultGroupName={#AppName}
DisableProgramGroupPage=yes
PrivilegesRequired=admin
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
OutputDir=..\dist\installer
OutputBaseFilename={#AppName}-{#AppVersion}-{#BuildStamp}-前端更新包
SetupIconFile=icon\app.ico
UninstallDisplayIcon={app}\{#AppExe}
UninstallDisplayName={#AppName}
Compression=lzma2/max
SolidCompression=yes
WizardStyle=modern
CloseApplications=yes
RestartApplications=no
ShowLanguageDialog=no

[Languages]
Name: "chinesesimplified"; MessagesFile: "lang\ChineseSimplified.isl"

[InstallDelete]
; 只清前端目录：旧 hash chunk 不残留（新 index.html 引用新文件名，残留只会占空间，
; 但清掉可避免手工核对时的误判）。_internal 其余模块、data/、logs/、config.yaml 一律不动。
Type: filesandordirs; Name: "{app}\_internal\web\dist"

[Files]
; 仅前端静态产物；其余文件由全量安装包负责
Source: "{#DistDir}\_internal\web\dist\*"; DestDir: "{app}\_internal\web\dist"; Flags: ignoreversion recursesubdirs createallsubdirs

[Code]
{ 补丁包必须装在已安装的程序目录上：未检测到主程序时直接拦下，避免凭空建目录 }
function PrepareToInstall(var NeedsRestart: Boolean): String;
begin
  Result := '';
  if not FileExists(ExpandConstant('{app}\{#AppExe}')) then
    Result := '未在目标目录检测到 {#AppExe}，请先安装完整安装包（或选择正确的安装目录）。';
end;
