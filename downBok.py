import asyncio
import aiohttp
import aiofiles
import os
import json
from pathlib import Path


async def download_file(url, save_path, retry_limit=3):
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
        await asyncio.sleep(2 + attempt)  # فترات انتظار بين المحاولات
    return False


async def log_error(file_name, book_data):
    """تسجيل الكتب التي فشل معالجتها في ملف JSON."""
    async with aiofiles.open(file_name, "a", encoding="utf-8") as f:
        await f.write(json.dumps(book_data, ensure_ascii=False) + "\n")


async def process_book(book_data, folder_path, semaphore):
    """معالجة كتاب: التنزيل وتسجيل الأخطاء إن وجدت."""
    async with semaphore:
        book_name = book_data["Book name"].replace(" ", "_").replace(":", "").replace("/", "").replace("\\", "")
        file_url = book_data["Download PDF"]
        save_path = folder_path / f"{book_name}.pdf"

        # محاولة تنزيل الكتاب
        is_downloaded = await download_file(file_url, save_path)
        if not is_downloaded:
            print(f"❌ فشل تنزيل الكتاب: {book_name}")
            await log_error("download_errors.json", book_data)


async def load_books(json_file):
    """تحميل بيانات الكتب من ملف JSON."""
    try:
        async with aiofiles.open(json_file, 'r', encoding='utf-8') as f:
            content = await f.read()
            return json.loads(content)
    except Exception as e:
        print(f"❌ فشل فتح ملف JSON: {e}")
        return []


async def main():
    # إعداد المجلد لحفظ الكتب
    folder_path = Path("AllBook")
    folder_path.mkdir(exist_ok=True)  # إنشاء المجلد إذا لم يكن موجودًا

    # تحميل بيانات الكتب
    json_file = "books.json"
    books = await load_books(json_file)
    if not books:
        print("❌ لم يتم العثور على بيانات الكتب!")
        return

    # تجهيز Semaphore لمعالجة متعددة (عدد الخيوط: 10)
    semaphore = asyncio.Semaphore(10)

    # بدء معالجة الكتب
    tasks = [
        process_book(book, folder_path, semaphore)
        for book in books
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    import nest_asyncio
    nest_asyncio.apply()
    asyncio.run(main())
