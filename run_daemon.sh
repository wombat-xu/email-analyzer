#!/bin/bash
# 邮件下载守护脚本 - 自动监控、崩溃重启、防休眠，直到全部下载完毕
# 用法: bash run_daemon.sh

cd "$(dirname "$0")"
DB="data/emails.db"
LOG="data/worker.log"
CHECK_INTERVAL=120  # 每2分钟检查一次

echo "========================================" | tee -a "$LOG"
echo "[$(date)] 守护脚本启动" | tee -a "$LOG"
echo "========================================" | tee -a "$LOG"

while true; do
    # 检查是否还有邮件需要下载
    REMAINING=$(sqlite3 "$DB" "SELECT COALESCE(SUM(total_on_server - fetched_count), 0) FROM sync_status WHERE total_on_server > fetched_count;" 2>/dev/null)
    TOTAL=$(sqlite3 "$DB" "SELECT COUNT(*) FROM emails;" 2>/dev/null)

    # 如果剩余为0或负数，说明全部下载完毕
    if [ -n "$REMAINING" ] && [ "$REMAINING" -le 0 ] 2>/dev/null; then
        echo "[$(date)] 所有邮件已下载完毕！数据库共 $TOTAL 封" | tee -a "$LOG"
        break
    fi

    # 检查下载进程是否在运行（-i 忽略大小写，macOS 的 Python 进程名首字母大写）
    PID=$(pgrep -fi "python.*run_full_download" | head -1)

    if [ -z "$PID" ]; then
        echo "[$(date)] 下载进程未运行，剩余 $REMAINING 封，正在启动..." | tee -a "$LOG"

        # 启动下载（后台运行）
        python3 run_full_download.py &
        DOWNLOAD_PID=$!

        # 绑定 caffeinate 防休眠
        caffeinate -i -w $DOWNLOAD_PID &
        echo "[$(date)] 下载进程已启动 PID=$DOWNLOAD_PID，caffeinate 已绑定" | tee -a "$LOG"

        # 等待较长时间让进程完成初始扫描
        sleep $CHECK_INTERVAL
    else
        # 进程在运行，用 CPU 时间判断是否真的卡住
        # 扫描已有邮件时邮件数不变但 CPU 在工作是正常的
        CPU_TIME_BEFORE=$(ps -p $PID -o time= 2>/dev/null | tr -d ' ')
        sleep $CHECK_INTERVAL
        CPU_TIME_AFTER=$(ps -p $PID -o time= 2>/dev/null | tr -d ' ')
        COUNT_AFTER=$(sqlite3 "$DB" "SELECT COUNT(*) FROM emails;" 2>/dev/null)

        if [ -z "$CPU_TIME_AFTER" ]; then
            # 进程已死
            echo "[$(date)] 下载进程 PID=$PID 已退出，等待下一轮重启" | tee -a "$LOG"
            sleep 5
            continue
        fi

        if [ "$CPU_TIME_BEFORE" = "$CPU_TIME_AFTER" ]; then
            # CPU 时间完全没变，说明进程真的卡死了（不是在扫描）
            # 再确认一次
            sleep $CHECK_INTERVAL
            CPU_TIME_FINAL=$(ps -p $PID -o time= 2>/dev/null | tr -d ' ')

            if [ "$CPU_TIME_BEFORE" = "$CPU_TIME_FINAL" ]; then
                echo "[$(date)] 进程 PID=$PID 已卡死（CPU无活动 ${CHECK_INTERVAL}x2 秒），终止并重启" | tee -a "$LOG"
                kill $PID 2>/dev/null
                sleep 5
            else
                echo "[$(date)] 下载正常（CPU活跃），当前 $COUNT_AFTER 封，剩余约 $REMAINING 封" | tee -a "$LOG"
            fi
        else
            echo "[$(date)] 下载正常（CPU活跃），当前 $COUNT_AFTER 封，剩余约 $REMAINING 封" | tee -a "$LOG"
        fi
    fi
done

echo "[$(date)] 守护脚本结束，邮件全部下载完毕！" | tee -a "$LOG"
