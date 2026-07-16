# manage_banned.py - CLI для управления списком запрещенных категорий
# Требование задания: "Настройте возможность управления списком через интерфейс командной строки"

import json
import sys
import os


BANNED_FILE = '../data/banned.json'


def load_banned():
    """Загрузить список запрещенных категорий"""
    if not os.path.exists(BANNED_FILE):
        return set()
    with open(BANNED_FILE, 'r', encoding='utf-8') as f:
        return set(json.load(f))


def save_banned(banned):
    """Сохранить список запрещенных категорий"""
    with open(BANNED_FILE, 'w', encoding='utf-8') as f:
        json.dump(list(banned), f, ensure_ascii=False, indent=2)
    print(f"✅ Сохранено в {BANNED_FILE}")


def show_banned():
    """Показать текущий список"""
    banned = load_banned()
    print(f"\n📋 Текущий список запрещенных категорий ({len(banned)}):")
    for i, cat in enumerate(sorted(banned), 1):
        print(f"  {i}. {cat}")
    print()


def add_category(category):
    """Добавить категорию"""
    banned = load_banned()
    if category in banned:
        print(f"⚠️  Категория '{category}' уже в списке")
    else:
        banned.add(category)
        save_banned(banned)
        print(f"✅ Добавлена категория: {category}")


def remove_category(category):
    """Удалить категорию"""
    banned = load_banned()
    if category not in banned:
        print(f"⚠️  Категория '{category}' не найдена в списке")
    else:
        banned.remove(category)
        save_banned(banned)
        print(f"✅ Удалена категория: {category}")


def main():
    """CLI интерфейс"""
    if len(sys.argv) < 2:
        print("Использование:")
        print("  python3 manage_banned.py show                    - показать список")
        print("  python3 manage_banned.py add <категория>         - добавить категорию")
        print("  python3 manage_banned.py remove <категория>      - удалить категорию")
        print("\nПримеры:")
        print("  python3 manage_banned.py show")
        print("  python3 manage_banned.py add 'Наркотики'")
        print("  python3 manage_banned.py remove 'Табак'")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == 'show':
        show_banned()
    elif command == 'add':
        if len(sys.argv) < 3:
            print("❌ Укажите название категории")
            sys.exit(1)
        category = sys.argv[2]
        add_category(category)
    elif command == 'remove':
        if len(sys.argv) < 3:
            print("❌ Укажите название категории")
            sys.exit(1)
        category = sys.argv[2]
        remove_category(category)
    else:
        print(f"❌ Неизвестная команда: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
