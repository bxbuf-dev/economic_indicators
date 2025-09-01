#!/bin/bash

# update_gdp.sh - Скрипт обновления данных ВВП США
# Использование: ./update_gdp.sh [--check-only] [--force-historical]

set -e  # Остановка при ошибке

# Определение директорий
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
COLLECTORS_DIR="$PROJECT_DIR/collectors"
VENV_DIR="$PROJECT_DIR/.venv"

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Функция вывода с цветом
print_status() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Проверка виртуального окружения
check_venv() {
    if [ ! -d "$VENV_DIR" ]; then
        print_error "Виртуальное окружение не найдено в $VENV_DIR"
        exit 1
    fi
    
    if [ ! -f "$VENV_DIR/bin/python" ]; then
        print_error "Python не найден в виртуальном окружении"
        exit 1
    fi
    
    print_success "Виртуальное окружение найдено"
}

# Проверка необходимых файлов
check_files() {
    local files=(
        "$PROJECT_DIR/.env"
        "$COLLECTORS_DIR/gdp_collector.py"
        "$COLLECTORS_DIR/gdp_revision_monitor.py"
        "$PROJECT_DIR/dao.py"
    )
    
    for file in "${files[@]}"; do
        if [ ! -f "$file" ]; then
            print_error "Файл не найден: $file"
            exit 1
        fi
    done
    
    print_success "Все необходимые файлы найдены"
}

# Активация виртуального окружения
activate_venv() {
    source "$VENV_DIR/bin/activate"
    print_success "Виртуальное окружение активировано"
}

# Проверка ревизий
check_revisions() {
    print_status "🔍 Проверка ревизий данных ВВП..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    python "$COLLECTORS_DIR/gdp_revision_monitor.py"
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    print_success "Проверка ревизий завершена"
}

# Обновление данных
update_data() {
    print_status "📊 Обновление данных ВВП..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    python "$COLLECTORS_DIR/gdp_collector.py"
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    print_success "Обновление данных завершено"
}

# Полная историческая загрузка
historical_load() {
    print_status "📈 Историческая загрузка данных ВВП..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    python "$COLLECTORS_DIR/gdp_historical_loader.py"
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    print_success "Историческая загрузка завершена"
}

# Показать помощь
show_help() {
    echo "Использование: $0 [опции]"
    echo ""
    echo "Опции:"
    echo "  --check-only         Только проверка ревизий (без обновления)"
    echo "  --force-historical   Принудительная полная загрузка истории"
    echo "  --help              Показать эту справку"
    echo ""
    echo "Примеры:"
    echo "  $0                   Проверка ревизий + обновление данных"
    echo "  $0 --check-only      Только проверка ревизий"
    echo "  $0 --force-historical Полная загрузка истории"
}

# Главная функция
main() {
    echo "════════════════════════════════════════════════════"
    echo "🏛️  ОБНОВЛЕНИЕ ДАННЫХ ВВП США (GDPC1)"
    echo "════════════════════════════════════════════════════"
    
    # Проверки
    check_venv
    check_files
    activate_venv
    
    # Обработка аргументов
    case "${1:-}" in
        --help|-h)
            show_help
            exit 0
            ;;
        --check-only)
            check_revisions
            ;;
        --force-historical)
            historical_load
            ;;
        "")
            # Стандартный сценарий: проверка + обновление
            check_revisions
            echo ""
            update_data
            ;;
        *)
            print_error "Неизвестная опция: $1"
            show_help
            exit 1
            ;;
    esac
    
    echo ""
    echo "════════════════════════════════════════════════════"
    print_success "Операция завершена успешно!"
    echo "════════════════════════════════════════════════════"
}

# Обработка прерывания (Ctrl+C)
trap 'print_warning "Операция прервана пользователем"; exit 130' INT TERM

# Запуск
main "$@"