import os
import sys
import json
import re
import subprocess
import requests
import httpx
import shutil
from urllib.parse import urlparse
from base64 import b64decode, b64encode
from pywidevine.cdm import Cdm
from pywidevine.device import Device
from pywidevine.pssh import PSSH


class KinescopeLogic:
    def __init__(self, log_callback=None):
        self.log_callback = log_callback
        self.log("[INIT] Инициализация KinescopeLogic...")
        
        # Определяем пути
        self.bin_dir = self._get_bin_path()
        self.wvd_path = self._get_wvd_path()
        
        self.log(f"[INIT] Путь к бинарникам: {self.bin_dir}")
        self.log(f"[INIT] Путь к WVD файлу: {self.wvd_path}")
        
        # Проверяем наличие бинарников
        self._check_binaries()
    
    def log(self, message):
        """Логирование с callback"""
        if self.log_callback:
            self.log_callback(message)
        else:
            print(message, flush=True)
    
    def _get_bin_path(self):
        """Определяет путь к бинарникам"""
        # Для Render
        if os.path.exists("/opt/render/project/src/bin"):
            return "/opt/render/project/src/bin"
        # Для локальной разработки (Windows)
        local_bin = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bin")
        if os.path.exists(local_bin):
            return local_bin
        # Если папки bin нет, создаем
        os.makedirs(local_bin, exist_ok=True)
        return local_bin
    
    def _get_wvd_path(self):
        """Определяет путь к WVD файлу"""
        # Для Render
        if os.path.exists("/opt/render/project/src/WVD.wvd"):
            return "/opt/render/project/src/WVD.wvd"
        # Для локальной разработки
        local_wvd = os.path.join(os.path.dirname(os.path.abspath(__file__)), "WVD.wvd")
        if os.path.exists(local_wvd):
            return local_wvd
        # Если файла нет, возвращаем путь где он должен быть
        return local_wvd
    
    def _check_binaries(self):
        """Проверяет наличие бинарников"""
        binaries = ["ffmpeg", "mp4decrypt", "N_m3u8DL-RE"]
        
        # На Windows добавляем .exe
        if sys.platform == "win32":
            binaries = [f"{b}.exe" for b in binaries]
        
        for binary in binaries:
            binary_path = os.path.join(self.bin_dir, binary)
            # Проверяем также в системном PATH (для ffmpeg который установлен через apt)
            if not os.path.exists(binary_path):
                # Проверяем в PATH
                system_path = shutil.which(binary.replace('.exe', ''))
                if system_path:
                    self.log(f"[INIT] ✓ {binary} найден в системе: {system_path}")
                else:
                    self.log(f"[INIT] ⚠️ {binary} не найден, будет использован системный")
            else:
                self.log(f"[INIT] ✓ {binary} найден: {binary_path}")
                # Делаем исполняемым на Linux
                if sys.platform != "win32":
                    os.chmod(binary_path, 0o755)
    
    def _get_ffmpeg_path(self):
        """Возвращает путь к ffmpeg"""
        # Сначала проверяем в папке bin
        ffmpeg_path = os.path.join(self.bin_dir, "ffmpeg")
        if sys.platform == "win32":
            ffmpeg_path = os.path.join(self.bin_dir, "ffmpeg.exe")
        
        if os.path.exists(ffmpeg_path):
            return ffmpeg_path
        
        # Затем проверяем в системе
        system_ffmpeg = shutil.which("ffmpeg")
        if system_ffmpeg:
            return system_ffmpeg
        
        # Если нет, возвращаем просто "ffmpeg" (надеемся что в PATH)
        return "ffmpeg"
    
    def _get_mp4decrypt_path(self):
        """Возвращает путь к mp4decrypt"""
        mp4decrypt_path = os.path.join(self.bin_dir, "mp4decrypt")
        if sys.platform == "win32":
            mp4decrypt_path = os.path.join(self.bin_dir, "mp4decrypt.exe")
        
        if os.path.exists(mp4decrypt_path):
            return mp4decrypt_path
        
        system_mp4decrypt = shutil.which("mp4decrypt")
        if system_mp4decrypt:
            return system_mp4decrypt
        
        return mp4decrypt_path
    
    def _get_n_m3u8dl_path(self):
        """Возвращает путь к N_m3u8DL-RE"""
        n_m3u8dl_path = os.path.join(self.bin_dir, "N_m3u8DL-RE")
        if sys.platform == "win32":
            n_m3u8dl_path = os.path.join(self.bin_dir, "N_m3u8DL-RE.exe")
        
        if os.path.exists(n_m3u8dl_path):
            return n_m3u8dl_path
        
        system_n_m3u8dl = shutil.which("N_m3u8DL-RE")
        if system_n_m3u8dl:
            return system_n_m3u8dl
        
        return n_m3u8dl_path

    def extract_from_json(self, json_filepath):
        """Извлекает данные видео из JSON файла"""
        self.log(f"[JSON] Чтение JSON файла: {json_filepath}")
        
        try:
            with open(json_filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            self.log(f"[JSON] ❌ Ошибка чтения JSON: {e}")
            raise
        
        video_url = data.get('url', '')
        referer = data.get('referrer', '')
        self.log(f"[JSON] URL видео: {video_url[:80]}...")
        self.log(f"[JSON] Referer: {referer}")
        
        results = []
        playlist = data.get('options', {}).get('playlist', [])
        
        if isinstance(playlist, list) and len(playlist) > 0:
            self.log(f"[JSON] Найден плейлист с {len(playlist)} видео")
            for idx, item in enumerate(playlist):
                video_title = item.get('title') or data.get('meta', {}).get('title', f'video_{idx + 1}')
                self.log(f"[JSON] [{idx + 1}/{len(playlist)}] Видео: {video_title}")
                results.append({
                    "url": video_url,
                    "referer": referer,
                    "title": video_title,
                    "video_data": item,
                    "full_data": data
                })
        else:
            self.log("[JSON] Плейлист не найден, используем основные данные")
            video_title = data.get('meta', {}).get('title', 'video')
            results.append({
                "url": video_url,
                "referer": referer,
                "title": video_title,
                "video_data": data,
                "full_data": data
            })
        
        self.log(f"[JSON] Извлечено {len(results)} элементов для скачивания")
        return results
    
    def get_key(self, pssh, license_url, referer):
        """Получает ключ дешифрования через Widevine CDM"""
        self.log("[WIDEVINE] === НАЧАЛО ПОЛУЧЕНИЯ КЛЮЧА ===")
        
        # Шаг 1: Проверка WVD файла
        self.log(f"[WIDEVINE] Шаг 1: Проверка наличия WVD файла: {self.wvd_path}")
        if not os.path.exists(self.wvd_path):
            self.log(f"[WIDEVINE] ❌ Файл {self.wvd_path} не найден. DRM методы недоступны.")
            return []
        self.log(f"[WIDEVINE] ✓ WVD файл найден")
        
        # Шаг 2: Настройка заголовков
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'origin': referer,
            'referer': referer
        }
        
        try:
            # Шаг 3: Загрузка устройства
            self.log("[WIDEVINE] Шаг 3: Загрузка устройства из WVD файла...")
            device = Device.load(self.wvd_path)
            self.log(f"[WIDEVINE] ✓ Устройство загружено (System ID: {device.system_id})")
            
            # Шаг 4: Инициализация CDM
            self.log("[WIDEVINE] Шаг 4: Инициализация CDM...")
            cdm = Cdm.from_device(device)
            
            # Шаг 5: Открытие сессии
            self.log("[WIDEVINE] Шаг 5: Открытие сессии CDM...")
            session_id = cdm.open()
            self.log(f"[WIDEVINE] ✓ Сессия открыта")
            
            # Шаг 6: Парсинг PSSH
            self.log(f"[WIDEVINE] Шаг 6: Парсинг PSSH...")
            pssh_obj = PSSH(pssh)
            self.log(f"[WIDEVINE] ✓ PSSH распознан")
            
            # Шаг 7: Генерация челленджа
            self.log("[WIDEVINE] Шаг 7: Генерация лицензионного челленджа...")
            challenge = cdm.get_license_challenge(session_id, pssh_obj)
            self.log(f"[WIDEVINE] ✓ Челлендж сгенерирован ({len(challenge)} байт)")
            
            # Шаг 8: Отправка запроса
            self.log(f"[WIDEVINE] Шаг 8: Отправка запроса на лицензионный сервер...")
            response = httpx.post(license_url, data=challenge, headers=headers, timeout=30)
            self.log(f"[WIDEVINE] ✓ Ответ получен (статус: {response.status_code})")
            
            if response.status_code != 200:
                self.log(f"[WIDEVINE] ❌ Сервер вернул ошибку: {response.status_code}")
                cdm.close(session_id)
                return []
            
            # Шаг 9: Парсинг лицензии
            self.log("[WIDEVINE] Шаг 9: Парсинг лицензии...")
            cdm.parse_license(session_id, response.content)
            self.log("[WIDEVINE] ✓ Лицензия успешно распарсена")
            
            # Шаг 10: Извлечение ключей
            keys = [f"{key.kid.hex}:{key.key.hex()}" for key in cdm.get_keys(session_id) if key.type == 'CONTENT']
            self.log(f"[WIDEVINE] Шаг 10: Найдено {len(keys)} ключей CONTENT")
            
            if keys:
                for i, key_str in enumerate(keys[:3], 1):  # Показываем первые 3
                    kid, key = key_str.split(':')
                    self.log(f"[WIDEVINE]   Ключ #{i}: KID={kid[:16]}... KEY={key[:16]}...")
            
            # Шаг 11: Закрытие сессии
            cdm.close(session_id)
            self.log("[WIDEVINE] ✓ Сессия закрыта")
            
            self.log("[WIDEVINE] === КЛЮЧИ УСПЕШНО ПОЛУЧЕНЫ ===")
            return keys
            
        except Exception as e:
            self.log(f"[WIDEVINE] ❌ Ошибка при получении ключа: {type(e).__name__}: {str(e)}")
            import traceback
            self.log(f"[WIDEVINE] Трассировка: {traceback.format_exc()}")
            return []
    
    def _extract_stream_urls(self, data):
        """Извлекает URL потоков (MPD и M3U8) из данных видео"""
        self.log("[STREAM] Извлечение URL потоков...")
        mpd_url, m3u8_url = None, None
        
        # Пробуем извлечь из sources
        sources = data.get('sources', [])
        
        if isinstance(sources, list):
            for s in sources:
                src = s.get('src', '')
                mime = s.get('type', '')
                
                if 'master.mpd' in src or 'manifest.mpd' in src or mime == 'application/dash+xml':
                    mpd_url = src
                    self.log(f"[STREAM] ✓ Найден MPD")
                if 'master.m3u8' in src or 'manifest.m3u8' in src or mime == 'application/x-mpegURL':
                    m3u8_url = src
                    self.log(f"[STREAM] ✓ Найден M3U8")
        
        elif isinstance(sources, dict):
            mpd_url = sources.get('shakadash', {}).get('src')
            m3u8_url = sources.get('hls', {}).get('src')
            if mpd_url:
                self.log(f"[STREAM] ✓ Найден MPD (shakadash)")
            if m3u8_url:
                self.log(f"[STREAM] ✓ Найден M3U8 (hls)")
        
        # Если есть только M3U8, пробуем угадать MPD
        if not mpd_url and m3u8_url:
            mpd_url = m3u8_url.replace('.m3u8', '.mpd')
            self.log(f"[STREAM] ⚠️ MPD угадан по M3U8")
        
        return mpd_url, m3u8_url
    
    def run_n_m3u8dl(self, url, keys, quality, save_dir, save_name, method_name):
        """Запускает N_m3u8DL-RE для скачивания видео"""
        self.log(f"[DOWNLOAD] === ЗАПУСК СКАЧИВАНИЯ ({method_name}) ===")
        self.log(f"[DOWNLOAD] Качество: {quality}p")
        self.log(f"[DOWNLOAD] Директория: {save_dir}")
        
        n_m3u8dl_path = self._get_n_m3u8dl_path()
        
        if not os.path.exists(n_m3u8dl_path):
            self.log(f"[DOWNLOAD] ⚠️ N_m3u8DL-RE не найден, пробуем использовать системный")
            n_m3u8dl_path = "N_m3u8DL-RE"
        
        # Формирование параметров ключей
        key_params = ""
        if keys:
            self.log(f"[DOWNLOAD] Используется {len(keys)} ключей")
            key_params = " ".join([f"--key {k}" for k in keys])
        
        # Очистка имени файла
        save_name_clean = re.sub(r'[^\w\s-]', '', save_name)
        save_name_clean = re.sub(r'[\s\\/:*?"<>|]', '_', save_name_clean).strip('_')
        
        # Формирование команды
        command = f'"{n_m3u8dl_path}" "{url}" {key_params} -M format=mp4 -sv best -sa ru --log-level INFO --save-dir "{save_dir}" --save-name "{save_name_clean}"'
        
        # Настройка окружения для ffmpeg
        env = os.environ.copy()
        ffmpeg_path = self._get_ffmpeg_path()
        ffmpeg_dir = os.path.dirname(ffmpeg_path)
        env["PATH"] = f"{ffmpeg_dir};{self.bin_dir};" + env.get("PATH", "")
        
        self.log(f"[DOWNLOAD] Запуск процесса...")
        
        try:
            process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='utf-8',
                errors='replace',
                bufsize=1,
                env=env
            )
            
            for line in process.stdout:
                clean_line = line.strip()
                if clean_line:
                    # Показываем прогресс
                    if any(x in clean_line.lower() for x in ['download', 'merge', '%', 'complete']):
                        self.log(f"[N_m3u8DL] {clean_line[:150]}")
                    elif 'error' in clean_line.lower() or 'fail' in clean_line.lower():
                        self.log(f"[N_m3u8DL] ⚠️ {clean_line[:150]}")
            
            process.wait()
            success = process.returncode == 0
            
            if success:
                self.log(f"[DOWNLOAD] ✓ Скачивание успешно завершено ({method_name})")
                output_file = os.path.join(save_dir, f"{save_name_clean}.mp4")
                if os.path.exists(output_file):
                    size_mb = os.path.getsize(output_file) / (1024 * 1024)
                    self.log(f"[DOWNLOAD] Размер файла: {size_mb:.2f} MB")
            else:
                self.log(f"[DOWNLOAD] ❌ Ошибка (код {process.returncode})")
            
            return success
            
        except Exception as e:
            self.log(f"[DOWNLOAD] ❌ Исключение: {type(e).__name__}: {str(e)}")
            return False
    
    def download_pipeline(self, info, quality, output_path):
        """Основной конвейер скачивания видео"""
        video_title = info['title']
        self.log(f"\n{'='*60}")
        self.log(f"[PIPELINE] НАЧАЛО СКАЧИВАНИЯ: {video_title}")
        self.log(f"{'='*60}")
        
        video_item = info['video_data']
        referer = info['referer']
        save_dir = os.path.dirname(output_path)
        save_name = os.path.splitext(os.path.basename(output_path))[0]
        
        os.makedirs(save_dir, exist_ok=True)
        self.log(f"[PIPELINE] Запрошенное качество: {quality}p")
        
        # Извлечение потоков
        mpd_url, m3u8_url = self._extract_stream_urls(video_item)
        
        if not m3u8_url:
            self.log("[PIPELINE] ❌ M3U8 URL не найден")
            return False
        
        self.log(f"[PIPELINE] M3U8 найден, начинаем загрузку...")
        
        # === СПОСОБ 1: WIDEVINE DRM ===
        self.log("\n[PIPELINE] === ПОПЫТКА 1: WIDEVINE DRM ===")
        try:
            license_url = video_item.get('drm', {}).get('widevine', {}).get('licenseUrl')
            
            if license_url and mpd_url:
                self.log(f"[WIDEVINE] License URL найден")
                
                # Загружаем MPD для получения PSSH
                self.log("[WIDEVINE] Загрузка MPD...")
                mpd_response = requests.get(mpd_url, timeout=15)
                mpd_response.raise_for_status()
                
                # Извлекаем PSSH
                pssh_match = re.search(r'<cenc:pssh[^>]*>([^<]+)</cenc:pssh>', mpd_response.text)
                if not pssh_match:
                    pssh_match = re.search(r'<mspr:pro>[^<]*<\[CDATA\[([^\]]+)\]\]', mpd_response.text)
                
                if pssh_match:
                    pssh = pssh_match.group(1).strip()
                    self.log(f"[WIDEVINE] PSSH найден")
                    
                    keys = self.get_key(pssh, license_url, referer)
                    if keys:
                        self.log("[WIDEVINE] Ключи получены, запускаем скачивание...")
                        if self.run_n_m3u8dl(m3u8_url, keys, quality, save_dir, save_name, "Widevine"):
                            self.log(f"[PIPELINE] ✓ УСПЕХ!")
                            return True
                else:
                    self.log("[WIDEVINE] PSSH не найден в MPD")
            else:
                self.log("[WIDEVINE] License URL или MPD не найдены")
                
        except Exception as e:
            self.log(f"[WIDEVINE] Ошибка: {type(e).__name__}: {str(e)}")
        
        # === СПОСОБ 2: БЕЗ КЛЮЧЕЙ ===
        self.log("\n[PIPELINE] === ПОПЫТКА 2: БЕЗ КЛЮЧЕЙ ===")
        if self.run_n_m3u8dl(m3u8_url, [], quality, save_dir, save_name, "Keyless"):
            self.log(f"[PIPELINE] ✓ УСПЕХ! Видео без DRM")
            return True
        
        # === ВСЕ СПОСОБЫ ИСЧЕРПАНЫ ===
        self.log(f"\n{'='*60}")
        self.log(f"[PIPELINE] ❌ ВСЕ МЕТОДЫ НЕУДАЧНЫ")
        self.log(f"{'='*60}")
        return False