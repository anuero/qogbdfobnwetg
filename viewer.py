# app.py
import streamlit as st
import pandas as pd
import json
import os
import tempfile
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime
from b2sdk.v2 import InMemoryAccountInfo, B2Api
logging.getLogger('b2sdk').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)

st.set_page_config(page_title="UniReportViewer", layout="wide")

B2_APP_KEY_ID = st.secrets.get("B2_KEY_ID")
B2_APP_KEY = st.secrets.get("B2_APP_KEY")
B2_BUCKET_NAME = st.secrets.get("B2_BUCKET_NAME")

@st.cache_resource(show_spinner=False)
def get_b2_bucket():

    try:
        info = InMemoryAccountInfo()
        b2_api = B2Api(info)
        b2_api.authorize_account("production", B2_APP_KEY_ID, B2_APP_KEY)
        bucket = b2_api.get_bucket_by_name(B2_BUCKET_NAME)
        return bucket
    except Exception as e:
        return None

@st.cache_data(ttl=30, show_spinner=False)
def list_user_scans(_bucket, username: str) -> List[Dict[str, Any]]:
    if not _bucket:
        return []
    
    all_files = []
    try:
        for file_version, folder_name in _bucket.ls(fetch_count=1000):
            file_name = file_version.file_name
            if file_name.endswith(f"_{username}.json"):
                all_files.append({
                    "file_name": file_name,
                    "file_id": file_version.id_,
                    "upload_timestamp": file_version.upload_timestamp,
                    "size": file_version.size
                })
        
        all_files.sort(key=lambda x: x["upload_timestamp"], reverse=True)
    except Exception as e:
        st.error(f"Ошибка При Получении Списка Файлов: {e}")
    
    return all_files

@st.cache_data(ttl=300, show_spinner=False)
def download_scan_from_b2(_bucket, file_name: str) -> Optional[Dict[str, Any]]:
    if not _bucket:
        return None
    
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.json') as tmp_file:
            tmp_path = tmp_file.name
            download = _bucket.download_file_by_name(file_name)
            download.save_to(tmp_path)
        
        with open(tmp_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return data
    except Exception as e:
        st.error(f"Ошибка При Загрузке Файла Из Базы Данных: {e}")
        return None
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except Exception:
                pass  

def ensure_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]

def df_from_list_of_dicts(items: List[Dict], default_columns: List[str] = None) -> pd.DataFrame:
    if not items:
        if default_columns:
            return pd.DataFrame(columns=default_columns)
        return pd.DataFrame()
    return pd.json_normalize(items)

def quick_filter_df(df: pd.DataFrame, q: str) -> pd.DataFrame:
    if not q or df.empty:
        return df
    q = q.lower()
    mask = pd.Series(False, index=df.index)
    for col in df.columns:
        try:
            mask = mask | df[col].astype(str).str.lower().str.contains(q, na=False)
        except Exception:
            continue
    return df[mask]

def display_archive_tree(entries: List[Dict[str, Any]]):
    if not isinstance(entries, list):
        st.warning("Файл не является архивом.")
        return

    if not entries:
        st.write("Нет Содержимого")
        return

    stack = [(entry, 0) for entry in reversed(entries)]

    while stack:
        current_entry, level = stack.pop()

        name = current_entry.get("Имя в архиве") or current_entry.get("Имя") or "<unknown>"
        is_dir = current_entry.get("Это папка", False)
        size = current_entry.get("size")
        
        indent = " " * (level * 4)  
        icon = '📁' if is_dir else '📄'
        header = f"{indent}{icon} {name}"
        
        if size is not None:
            header += f" — {size}"

        with st.expander(header, expanded=False):
            meta = {k: v for k, v in current_entry.items() if k not in ("Вложенное", "Имя в архиве", "Имя")}
            if meta:
                st.json(meta)
            
            nested_entries = current_entry.get("Вложенное")
            if isinstance(nested_entries, list) and nested_entries:
                for nested_entry in reversed(nested_entries):
                    stack.append((nested_entry, level + 1))


def format_timestamp(ts):
    try:
        dt = datetime.fromtimestamp(ts / 1000)  
        return dt.strftime("%d.%m.%Y %H:%M:%S")
    except:
        return str(ts)

def format_size(size_bytes):
    if size_bytes is None:
        return ""
    try:
        size_bytes = float(size_bytes)
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} TB"
    except (ValueError, TypeError):
        return ""


bucket = get_b2_bucket()

if not bucket:
    st.error("❌ Не Удалось Подключиться к Базе Данных. Проверьте подключение к интернету и попробуйте снова.")
    
    if st.button("🔄 Попробовать снова"):
        get_b2_bucket.clear()  
        st.rerun()  
        
    st.stop()

if 'username' not in st.session_state:
    st.session_state.username = ""
if 'selected_file' not in st.session_state:
    st.session_state.selected_file = None
if 'scan_data' not in st.session_state:
    st.session_state.scan_data = None
if 'current_section' not in st.session_state:
    st.session_state.current_section = "📊 Информация"

if st.session_state.scan_data:
    sections = [
        "📊 Информация",
        "🔧 Драйвера",
        "⚙️ Процессы",
        "🌐 Данные Браузеров",
        "⏳ Загрузки",
        "🖥️ Рабочий стол",
        "📂 Временные Файлы",
        "🗑️ Удаленные Файлы",
        "📦 Загруженные Модули и Файлы Процесса"
    ]
    st.session_state.current_section = st.sidebar.radio("Разделы Отчёта", sections)

show_setup = not st.session_state.scan_data or st.session_state.current_section == "📊 Информация"

if show_setup:
    st.subheader("1️⃣ Введите username")
    
    with st.form(key="user_search_form"):
        username_input = st.text_input(
            "Username Для Поиска Сканов:",
            value=st.session_state.get("username", ""),
            placeholder="Например: ivan"
        )
        
        submitted = st.form_submit_button("🔍 Поиск")

    if submitted:
        list_user_scans.clear()
        
        if username_input != st.session_state.username:
            st.session_state.username = username_input
            st.session_state.selected_file = None
            st.session_state.scan_data = None
        
        st.rerun()

    if st.session_state.username:
        st.subheader(f"2️⃣ Сканы Пользователя: {st.session_state.username}")
        
        with st.spinner("Загрузка Списка Файлов..."):
            user_files = list_user_scans(bucket, st.session_state.username)
        
        if not user_files:
            st.warning(f"⚠️ Файлы Для Пользователя '{st.session_state.username}' Не Найдены.")
        else:
            st.success(f"✅ Найдено Файлов: {len(user_files)}")
            
            display_files = []
            for f in user_files:
                display_files.append({
                    "Файл": "report.json",  
                    "Дата загрузки": format_timestamp(f["upload_timestamp"]),
                    "Размер": format_size(f["size"])
                })
            
            df_files = pd.DataFrame(display_files)
            st.dataframe(df_files, use_container_width=True, hide_index=True)
            
            file_options_display = [format_timestamp(f["upload_timestamp"]) for f in user_files]
            file_options_real = [f["file_name"] for f in user_files]
            
            current_index = 0
            if st.session_state.selected_file:
                try:
                    current_index = file_options_real.index(st.session_state.selected_file)
                except ValueError:
                    current_index = 0
            
            selected_display = st.selectbox(
                "Выберите Файл Для Просмотра:",
                options=file_options_display,
                index=current_index
            )
            
            selected_idx = file_options_display.index(selected_display)
            selected_real = file_options_real[selected_idx]
            
            if selected_real != st.session_state.selected_file:
                st.session_state.selected_file = selected_real
                st.session_state.scan_data = None
            
            if st.button("📥 Загрузить Выбранный Файл", type="primary"):
                with st.spinner(f"Загрузка Отчёта От {selected_display}..."):
                    data = download_scan_from_b2(bucket, selected_real)
                    if data:
                        st.session_state.scan_data = data
                        st.success("✅ Файл Успешно Загружен!")
                        st.rerun()

if st.session_state.scan_data:
    data = st.session_state.scan_data
    
    drivers_loaded = ensure_list(data.get("Загруженные драйверы"))
    drivers_dir_files = ensure_list(data.get("Файлы папки драйверов"))
    driverquery = ensure_list(data.get("Службы драйверов"))
    processes = ensure_list(data.get("Процессы"))
    modules = ensure_list(data.get("Модули"))
    process_dir_files = ensure_list(data.get("Файлы папки процесса"))
    browser_downloads = ensure_list(data.get("История браузеров"))
    file_snapshot = data.get("Снимок файловой системы") or {}
    downloads_files = ensure_list(file_snapshot.get("Файлы из Загрузок"))
    desktop_files = ensure_list(file_snapshot.get("Файлы рабочего стола"))
    appdata_files = ensure_list(file_snapshot.get("Файлы из AppData")) or ensure_list(data.get("Файлы из AppData"))
    deleted_files = ensure_list(file_snapshot.get("Удаленные файлы (Корзина)")) or ensure_list(data.get("Удаленные файлы (Корзина)"))
    
    sel = st.session_state.current_section
    
    if sel == "📊 Информация":
        st.header("📊 Информация")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Пользователь", data.get('Имя пользователя', 'N/A'))
        with col2:
            st.metric("Дата Сканирования", data.get('Время сканирования', 'N/A'))
        with col3:
            st.metric("Версия Windows", data.get('Версия Windows', 'N/A'))
        
        st.subheader("Быстрая Статистика")
        stats = {
            "Драйвера (Загруженные)": len(drivers_loaded),
            "Драйвера (В Папке)": len(drivers_dir_files),
            "Сервисы Драйверов": len(driverquery),
            "Процессы": len(processes),
            "Модули": len(modules),
            "Данные Браузеров": len(browser_downloads),
            "Загрузки": len(downloads_files),
            "Файлы на рабочем столе": len(desktop_files),
            "Временные Файлы": len(appdata_files),
            "Удалённые Файлы": len(deleted_files),
        }
        df_stats = pd.DataFrame.from_dict(stats, orient="index", columns=["Количество"])
        st.dataframe(df_stats, use_container_width=True)
        
    elif sel == "🔧 Драйвера":
        st.header("🔧 Драйвера")
        combined = []
        
        for d in drivers_loaded:
            combined.append({
                "Имя": d.get("Имя") or os.path.basename(d.get("Путь") or ""),
                "Путь": d.get("Путь"),
                "MD5": d.get("MD5"),
                **{k: v for k, v in d.items() if k not in ("Имя","Путь","MD5")}
            })
        for d in drivers_dir_files:
            combined.append({
                "Имя": d.get("Имя"),
                "Путь": d.get("Путь"),
                "MD5": d.get("MD5"),
                **{k:v for k,v in d.items() if k not in ("Имя","Путь","MD5")}
            })
        for d in driverquery:
            combined.append({
                "Имя Службы": d.get("Имя службы"),
                "Отображаемое Имя": d.get("Отображаемое имя"),
                "Состояние": d.get("Состояние"),
                "Путь К Файлу": d.get("Путь к файлу"),
                "MD5": d.get("MD5"),
                **{k:v for k,v in d.items() if k not in ("Имя службы","Отображаемое имя","Состояние","Путь к файлу","MD5")}
            })
        
        df_drv = df_from_list_of_dicts(combined)
        q = st.text_input("🔍 Фильтр По Драйверам", value="")
        filtered = quick_filter_df(df_drv, q)
        st.write(f"Показано {len(filtered)} Из {len(df_drv)} Записей")
        st.dataframe(filtered, use_container_width=True)
        
        if not filtered.empty:
            st.markdown("### 🔎 Детали Строки")
            row_idx = st.number_input("Номер Строки", min_value=0, max_value=max(0, len(filtered)-1), value=0)
            row = filtered.reset_index(drop=True).iloc[row_idx].to_dict()
            st.json(row)
            
    elif sel == "⚙️ Процессы":
        st.header("⚙️ Процессы")
        df_proc = df_from_list_of_dicts(processes, default_columns=["PID","Имя","Путь"])
        q = st.text_input("🔍 Фильтр По Процессам", value="")
        filtered = quick_filter_df(df_proc, q)
        st.write(f"Показано {len(filtered)} Из {len(df_proc)}")
        st.dataframe(filtered, use_container_width=True)
        
        st.markdown("### 🔎 Выбрать PID Для Подробной Информации")
        try:
            pids = list(map(int, filtered.get("PID", pd.Series([])).dropna().astype(int).unique()))
        except Exception:
            pids = []
        pid_choice = st.selectbox("PID", options=[None] + pids)
        if pid_choice:
            match = next((p for p in processes if int(p.get("PID", -1)) == int(pid_choice)), None)
            if match:
                st.json(match)
            
    elif sel == "🌐 Данные Браузеров":
        st.header("🌐 Данные Браузеров")
        df_bd = df_from_list_of_dicts(browser_downloads, default_columns=["Браузер", "Имя файла", "Путь к файлу", "URL источника"])
        q = st.text_input("🔍 Поиск В Загрузках", value="")
        filtered = quick_filter_df(df_bd, q)
        st.write(f"Показано {len(filtered)} Из {len(df_bd)}")
        st.dataframe(filtered, use_container_width=True)
        
        if not filtered.empty:
            st.markdown("### 🔎 Детали Строки")
            idx = st.number_input("Индекс", min_value=0, max_value=max(0, len(filtered)-1), value=0)
            st.json(filtered.reset_index(drop=True).iloc[idx].to_dict())
            
    elif sel == "⏳ Загрузки":
        st.header("⏳ Загрузки")
        df_dl = df_from_list_of_dicts(downloads_files)
        q = st.text_input("🔍 Поиск По Имени/Пути", value="")
        filtered = quick_filter_df(df_dl, q)
        st.write(f"Показано {len(filtered)} Из {len(df_dl)}")
        st.dataframe(filtered, use_container_width=True)
        
        if not filtered.empty:
            st.markdown("### 🔎 Просмотр Детали Аайла")
            idx = st.number_input("Индекс Файла", min_value=0, max_value=max(0, len(filtered)-1), value=0)
            file_rec = filtered.reset_index(drop=True).iloc[idx].to_dict()
            st.json(file_rec)
            archive_contents = file_rec.get("Содержание архива")
            if archive_contents:
                st.markdown("**Содержимое Архива:**")
                display_archive_tree(archive_contents)

    elif sel == "🖥️ Рабочий стол":
        st.header("🖥️ Рабочий стол")
        df_desk = df_from_list_of_dicts(desktop_files)
        q = st.text_input("🔍 Поиск по файлам на рабочем столе", value="")
        filtered = quick_filter_df(df_desk, q)
        st.write(f"Показано {len(filtered)} из {len(df_desk)} записей")
        st.dataframe(filtered, use_container_width=True)
        
        if not filtered.empty:
            st.markdown("### 🔎 Просмотр деталей файла")
            idx = st.number_input("Индекс Файла", min_value=0, max_value=max(0, len(filtered)-1), value=0, key="desktop_idx")
            file_rec = filtered.reset_index(drop=True).iloc[idx].to_dict()
            
            display_dict = {k: v for k, v in file_rec.items() if k != "Содержание архива"}
            st.json(display_dict)

            archive_contents = file_rec.get("Содержание архива")
            is_nan = (
                archive_contents is None
                or (isinstance(archive_contents, float) and pd.isna(archive_contents))
                or (isinstance(archive_contents, (list, dict, pd.Series, pd.DataFrame)) and len(archive_contents) == 0)
            )

            if not is_nan:
                st.markdown("**Содержимое Архива:**")
                display_archive_tree(archive_contents)
            else:
                st.warning("Файл не является архивом.")

            
    elif sel == "📂 Временные Файлы":
        st.header("📂 Временные файлы")
        df_ad = df_from_list_of_dicts(appdata_files)
        q = st.text_input("🔍 Поиск в AppData", value="")
        filtered = quick_filter_df(df_ad, q)
        st.write(f"Показано {len(filtered)} Из {len(df_ad)}")
        st.dataframe(filtered, use_container_width=True)
        
        if not filtered.empty:
            idx = st.number_input("Индекс", min_value=0, max_value=max(0, len(filtered)-1), value=0)
            rec = filtered.reset_index(drop=True).iloc[idx].to_dict()
            st.json(rec)
            archive_contents = rec.get("Содержание архива")
            if archive_contents:
                st.markdown("Содержимое Архива:")
                display_archive_tree(archive_contents)
            
    elif sel == "🗑️ Удаленные Файлы":
        st.header("🗑️ Удалённые файлы")
        df_del = df_from_list_of_dicts(deleted_files)
        q = st.text_input("🔍 Поиск В Удалённых Файлах", value="")
        filtered = quick_filter_df(df_del, q)
        st.write(f"Показано {len(filtered)} Из {len(df_del)}")
        st.dataframe(filtered, use_container_width=True)
        
        if not filtered.empty:
            idx = st.number_input("Индекс", min_value=0, max_value=max(0, len(filtered)-1), value=0)
            rec = filtered.reset_index(drop=True).iloc[idx].to_dict()
            st.json(rec)
            archive_contents = rec.get("Содержание архива")
            if archive_contents:
                st.markdown("Содержимое Архива:")
                display_archive_tree(archive_contents)
            
    elif sel == "📦 Загруженные Модули и Файлы Процесса":
        st.header("📦 Загруженные Модули и Файлы Процесса")
        st.subheader("Список Загруженных Модулей")
        df_mod = df_from_list_of_dicts([{"Путь": m} for m in modules])
        st.dataframe(df_mod, use_container_width=True)
        
        st.subheader("Список Файлов Процесса")
        df_pdf = df_from_list_of_dicts(process_dir_files)
        st.dataframe(df_pdf, use_container_width=True)