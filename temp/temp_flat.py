import os
import shutil

# 1. Zotero에서 내보낸 루트 폴더 경로를 여기에 적어줘
ROOT_DIR = r"C:\Users\parkj\Downloads\특용작물 스마트팜"

# 2. 파일을 모두 모을 대상 폴더 이름 (루트 안에 자동 생성됨)
FLAT_DIR = os.path.join(ROOT_DIR, "_flat")

# 모을 파일 확장자 (PDF만 하고 싶으면 ['.pdf'] 로 줄이면 됨)
TARGET_EXTS = [".pdf", ".docx", ".pptx", ".xlsx"]  # 필요에 맞게 수정


def ensure_flat_dir():
    os.makedirs(FLAT_DIR, exist_ok=True)


def should_move_file(file_path):
    # 확장자 필터링
    ext = os.path.splitext(file_path)[1].lower()
    return ext in TARGET_EXTS


def unique_name(target_dir, filename):
    """
    target_dir에 filename이 이미 있으면, 이름 뒤에 _1, _2 ... 붙여서 충돌 피하기
    """
    base, ext = os.path.splitext(filename)
    candidate = filename
    counter = 1
    while os.path.exists(os.path.join(target_dir, candidate)):
        candidate = f"{base}_{counter}{ext}"
        counter += 1
    return candidate


def flatten_files():
    ensure_flat_dir()

    for dirpath, dirnames, filenames in os.walk(ROOT_DIR):
        # 이미 모아두는 _flat 폴더는 건너뛰기
        if os.path.abspath(dirpath) == os.path.abspath(FLAT_DIR):
            continue

        for fname in filenames:
            src = os.path.join(dirpath, fname)

            # 혹시라도 파일이 아닌 건 제외
            if not os.path.isfile(src):
                continue

            # 확장자 필터링
            if not should_move_file(src):
                continue

            # 이름 충돌 방지용 새 이름 생성
            new_name = unique_name(FLAT_DIR, fname)
            dst = os.path.join(FLAT_DIR, new_name)

            print(f"Moving: {src} -> {dst}")
            shutil.move(src, dst)


def remove_empty_dirs():
    """
    파일을 다 옮긴 뒤, 루트 아래의 빈 폴더들을 지운다 (_flat 제외)
    """
    for dirpath, dirnames, filenames in os.walk(ROOT_DIR, topdown=False):
        # _flat 폴더는 남겨두기
        if os.path.abspath(dirpath) == os.path.abspath(FLAT_DIR):
            continue

        if not dirnames and not filenames:
            print(f"Removing empty dir: {dirpath}")
            os.rmdir(dirpath)


if __name__ == "__main__":
    # *** 아주 중요: 혹시 모르니 꼭 백업해두고 돌려! ***
    flatten_files()
    # 빈 폴더도 지우고 싶으면 아래 줄 주석 해제
    # remove_empty_dirs()
