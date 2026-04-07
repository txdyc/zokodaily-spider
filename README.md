

- 把单文件抓取器重构成可扩展框架：统一调度、站点注册、文章解析、翻译、图片下载、入库都拆到 news_crawler/runner.py:22、news_crawler/sites/base.py:13、news_crawler/http.py:13、news_crawler/images.py:16。

- 新增 GraphicSpider，已覆盖你给的 3 个栏目入口：news_crawler/sites/graphic.py:22。

- 保留并迁移了 MyJoy 抓取能力到新框架：news_crawler/sites/myjoy.py:27。

- 统一把所有站点新闻写入 zokodaily_news，并在初始化时自动建表/补列/调整唯一索引，避免多站点共表时被原来的 title+news_date 约束误伤：news_crawler/db.py:56、schema/zokodaily_news.sql:1。

- 图片会下载到本地 downloads/news_images/<site>/...，数据库里的 img 保存本地路径，便于前端直接加载：news_crawler/images.py:16。

入口

- 新总入口：news_crawler.py:1
- 兼容旧入口：myjoy_crawler.py:10
- 新增 Graphic 专用入口：graphic_crawler.py:10

我做的校验

- 已通过编译检查：python -m compileall news_crawler news_crawler.py myjoy_crawler.py graphic_crawler.py
- 已确认入口可正常导入，并且 news_crawler.py --list-sites 正常输出站点列表。
- 由于当前环境未实际放开网络抓取，我还没有执行真实的 graphic.com.gh 在线抓取验证。

运行方式

- 抓全部：.\.conda\python.exe news_crawler.py
- 只抓 Graphic：.\.conda\python.exe graphic_crawler.py
- 只抓 MyJoy：.\.conda\python.exe myjoy_crawler.py
- 指定站点：.\.conda\python.exe news_crawler.py --sites graphic,myjoyonline

自动运行

- 新闻爬虫单次运行脚本：`scripts/run_news_crawler.ps1`
- Jiji 房产爬虫单次运行脚本：`scripts/run_jiji_crawler.ps1`
- 注册 Windows 定时任务：`scripts/register_scheduled_tasks.ps1`
- 删除 Windows 定时任务：`scripts/unregister_scheduled_tasks.ps1`
- Ubuntu 新闻爬虫单次运行脚本：`scripts/run_news_crawler.sh`
- Ubuntu Jiji 房产爬虫单次运行脚本：`scripts/run_jiji_crawler.sh`
- 注册 Ubuntu cron 任务：`scripts/register_cron_jobs.sh`
- 删除 Ubuntu cron 任务：`scripts/unregister_cron_jobs.sh`

默认调度

- 新闻爬虫：每 1 小时运行一次
- Jiji 房产爬虫：每 2 小时运行一次
- 日志目录：`scheduler_logs/`

注册方式

- 在 PowerShell 中执行：`powershell -ExecutionPolicy Bypass -File .\scripts\register_scheduled_tasks.ps1`
- 默认会创建两个任务：
  - `NewsSpider-NewsCrawler-Hourly`
  - `NewsSpider-JijiCrawler-Every2Hours`

可选参数

- 自定义开始时间：
  - `powershell -ExecutionPolicy Bypass -File .\scripts\register_scheduled_tasks.ps1 -NewsStartTime 01:00 -JijiStartTime 01:30`
- 单次运行时可覆盖抓取参数，例如：
  - `powershell -ExecutionPolicy Bypass -File .\scripts\run_news_crawler.ps1 -Sites "myjoyonline,graphic" -MaxPages 3 -Concurrency 4`
  - `powershell -ExecutionPolicy Bypass -File .\scripts\run_jiji_crawler.ps1 -MaxPages 50 -Concurrency 6`

Ubuntu 24 定时运行

- 赋予执行权限：
  - `chmod +x ./scripts/run_news_crawler.sh ./scripts/run_jiji_crawler.sh ./scripts/register_cron_jobs.sh ./scripts/unregister_cron_jobs.sh`
- 注册 cron：
  - `./scripts/register_cron_jobs.sh`
- 删除 cron：
  - `./scripts/unregister_cron_jobs.sh`

Ubuntu 默认调度

- 新闻爬虫：每小时 `0` 分运行一次
- Jiji 房产爬虫：每两小时 `30` 分运行一次
- 为避免任务重叠，注册脚本会通过 `flock` 加锁
- 日志同样写入：`scheduler_logs/`

Ubuntu Python 解释器查找顺序

- 优先使用环境变量 `PYTHON_BIN`
- 其次使用 `./.conda/bin/python`
- 再次使用 `./.venv/bin/python`
- 最后回退到系统 `python3` 或 `python`
