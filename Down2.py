import asyncio
import aiohttp
from telegram import Bot
from telegram.error import TelegramError, RetryAfter, TimedOut
import os
import json
import aiofiles
from pathlib import Path


async def download_file(url, save_path, retry_limit=5):
    """تنزيل ملف مع إعادة المحاولة في حالة الفشل."""
    for attempt in range(retry_limit):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        async with aiofiles.open(save_path, 'wb') as f:
                            await f.write(await response.read())
                        print(f"✅ تم تنزيل الملف: {save_path}")
                        return True
                    else:
                        print(f"⚠️ خطأ في التنزيل - محاولة {attempt + 1}: {response.status}")
        except Exception as e:
            print(f"⚠️ استثناء أثناء التنزيل - محاولة {attempt + 1}: {e}")
        await asyncio.sleep(2 + attempt * 2)  # فترات انتظار متزايدة
    return False


async def send_file_to_channels(bot, channels, file_path, retry_limit=5):
    """إرسال ملف إلى جميع القنوات مع إعادة المحاولة في حالة الفشل."""
    for channel_id in channels:
        for attempt in range(retry_limit):
            try:
                with open(file_path, 'rb') as file:
                    await bot.send_document(chat_id=channel_id, document=file)
                print(f"✅ تم إرسال الملف إلى القناة {channel_id}")
                break  # الخروج من المحاولات إذا تم الإرسال بنجاح
            except RetryAfter as e:
                wait_time = int(e.retry_after) + 1
                print(f"⚠️ تم حظر البوت مؤقتًا. الانتظار {wait_time} ثانية...")
                await asyncio.sleep(wait_time)
            except (TimedOut, TelegramError) as e:
                print(f"⚠️ خطأ أثناء الإرسال إلى القناة {channel_id} - محاولة {attempt + 1}: {e}")
                await asyncio.sleep(2 + attempt * 2)  # فترات انتظار متزايدة
                if attempt == retry_limit - 1:
                    return False
    return True


async def log_error(file_name, book_data):
    """تسجيل الكتب التي فشل معالجتها في ملف JSON."""
    async with aiofiles.open(file_name, "a", encoding="utf-8") as f:
        await f.write(json.dumps(book_data, ensure_ascii=False) + "\n")


async def process_book(bot, channels, book_data, folder_path, semaphore):
    """معالجة كتاب: التنزيل، الإرسال، التحقق من الأخطاء."""
    async with semaphore:
        book_name = book_data["Book name"].replace(" ", "_").replace(":", "").replace("/", "").replace("\\", "")
        file_url = book_data["Download PDF"]
        save_path = folder_path / f"{book_name}.pdf"

        # محاولة تنزيل الكتاب
        is_downloaded = await download_file(file_url, save_path)
        if not is_downloaded:
            print(f"❌ فشل تنزيل الكتاب: {book_name}")
            await log_error("download_errors.json", book_data)
            return

        # محاولة إرسال الكتاب
        is_sent = await send_file_to_channels(bot, channels, save_path)
        if not is_sent:
            print(f"❌ فشل إرسال الكتاب: {book_name}")
            await log_error("upload_errors.json", book_data)
            return

        # حذف الملف بعد النجاح الكامل
        os.remove(save_path)
        print(f"🗑️ تم حذف الكتاب: {book_name}")


async def load_books(json_file):
    """تحميل بيانات الكتب من ملف JSON."""
    try:
        async with aiofiles.open(json_file, 'r', encoding='utf-8') as f:
            content = await f.read()
            return json.loads(content)
    except Exception as e:
        print(f"❌ فشل فتح ملف JSON: {e}")
        return []


async def save_remaining_books(books):
    """حفظ الكتب المتبقية إلى ملف."""
    async with aiofiles.open("remainingbooks.json", "w", encoding="utf-8") as f:
        await f.write(json.dumps(books, ensure_ascii=False, indent=2))


async def get_channels(bot):
    """الحصول على جميع القنوات التي يكون البوت مشرفًا بها."""
    channels = []
    try:
        updates = await bot.get_updates()
        for update in updates:
            if update.message and update.message.chat.type in ["group", "supergroup", "channel"]:
                channels.append(update.message.chat.id)
        return list(set(channels))  # إزالة التكرارات
    except Exception as e:
        print(f"⚠️ خطأ في جلب القنوات: {e}")
        return []


async def main():
    # إعدادات Telegram
    token = '7054415537:AAH7fZVGPlTt0xAU-V5hZHVQnikIi2bN0dE'
    bot = Bot(token=token)

    # جلب القنوات التي يكون البوت مشرفًا بها
    print("🔄 جلب القنوات التي يشرف عليها البوت...")
    channels = await get_channels(bot)
    if not channels:
        print("❌ لم يتم العثور على قنوات!")
        return
    print(f"✅ القنوات: {channels}")

    # تحميل بيانات الكتب
    json_file = "books.json"
    remaining_file = "remainingbooks.json"
    folder_path = Path("books")
    folder_path.mkdir(exist_ok=True)  # إنشاء المجلد إذا لم يكن موجودًا

    # تحديد الكتب للعمل
    books = []
    if os.path.exists(remaining_file):
        print("🔄 استئناف من الكتب المتبقية...")
        books = await load_books(remaining_file)
    else:
        print("📚 تحميل الكتب من الملف الأصلي...")
        books = await load_books(json_file)

    # تجهيز Semaphore لمعالجة 10 كتب في نفس الوقت
    semaphore = asyncio.Semaphore(10)

    # بدء معالجة الكتب
    try:
        tasks = [
            process_book(bot, channels, book, folder_path, semaphore)
            for book in books
        ]
        await asyncio.gather(*tasks)
    finally:
        # حفظ الكتب المتبقية عند التوقف
        remaining_books = [book for task, book in zip(tasks, books) if not task.done()]
        if remaining_books:
            print("💾 حفظ الكتب المتبقية...")
            await save_remaining_books(remaining_books)


if __name__ == "__main__":
    import nest_asyncio
    nest_asyncio.apply()
    asyncio.run(main())
