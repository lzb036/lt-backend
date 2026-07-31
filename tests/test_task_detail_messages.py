from __future__ import annotations

from types import SimpleNamespace

from app.services import crawler_service


def test_mysql_lock_error_is_presented_as_user_guidance() -> None:
    row = SimpleNamespace(
        error_detail=(
            "(pymysql.err.OperationalError) "
            "(1205, 'Lock wait timeout exceeded; try restarting transaction')\n"
            "[SQL: INSERT INTO lt_products (...)]\n"
            "[parameters: {'title': '商品'}]\n"
            "(Background on this error at: https://sqlalche.me/e/20/e3q8)"
        )
    )

    detail = crawler_service.task_public_error_detail(row)

    assert detail is not None
    assert "原因：数据库当时正在处理其他写入" in detail
    assert "影响：" in detail
    assert "处理建议：" in detail
    assert "INSERT INTO" not in detail
    assert "parameters" not in detail


def test_normal_crawl_skips_are_summarized_without_product_titles() -> None:
    row = SimpleNamespace(
        warning_detail=(
            "很长的日文商品标题一: 已存在于待审核商品，本次未重复入库。\n"
            "很长的日文商品标题二: 商品价格 1200 日元不符合用户设置的采集价格条件，已跳过。"
        )
    )

    detail = crawler_service.task_public_warning_detail(row, skipped_count=128)

    assert detail is not None
    assert "本次有 128 件商品未入库" in detail
    assert "已经采集" in detail
    assert "价格条件" in detail
    assert "很长的日文商品标题" not in detail


def test_clear_chinese_item_errors_remain_visible() -> None:
    detail = crawler_service.humanize_task_error_detail(
        "商品 62173：图片不存在或已失效（HTTP 404）。\n"
        "商品 62172：图片不存在或已失效（HTTP 404）。"
    )

    assert detail == (
        "商品 62173：图片不存在或已失效（HTTP 404）。\n"
        "商品 62172：图片不存在或已失效（HTTP 404）。"
    )


def test_product_number_does_not_trigger_http_status_mapping() -> None:
    detail = crawler_service.humanize_task_error_detail(
        "商品 401：图片不存在或已失效（HTTP 404）。"
    )

    assert detail == "商品 401：图片不存在或已失效（HTTP 404）。"


def test_non_skipped_warnings_are_also_summarized() -> None:
    row = SimpleNamespace(
        warning_detail="很长的商品标题: 图片本地化失败：下载图片超时。"
    )

    detail = crawler_service.task_public_warning_detail(row, warning_count=3)

    assert detail is not None
    assert "本次有 3 件商品需要注意" in detail
    assert "商品基础信息已经入库" in detail
    assert "很长的商品标题" not in detail


def test_unknown_technical_error_uses_safe_generic_guidance() -> None:
    detail = crawler_service.humanize_task_error_detail(
        "ValueError: unexpected parser state\n"
        "Traceback (most recent call last):\n"
        'File "/app/service.py", line 10, in run'
    )

    assert detail is not None
    assert "任务执行过程中发生系统异常" in detail
    assert "联系管理员并提供任务编号" in detail
    assert "service.py" not in detail


def test_empty_task_detail_remains_empty() -> None:
    assert crawler_service.humanize_task_error_detail("") is None
    assert crawler_service.humanize_task_warning_detail("", skipped_count=10) is None
