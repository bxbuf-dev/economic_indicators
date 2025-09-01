#!/bin/bash

# update_all_indicators.sh - Универсальный скрипт обновления всех экономических индикаторов
# Использование: ./update_all_indicators.sh [--quick] [--verbose]

set -e

# Определение директорий
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
COLLECTORS_DIR="$PROJECT_DIR/collectors"
VENV_DIR="$PROJECT_DIR/.venv"

# Цвета
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Переменные
VERBOSE=false
QUICK_MODE=false

# Функции вывода
print_header() {
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN} $1${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
}

print_status() {
    echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $1"
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

# Список коллекторов с описаниями
declare -A COLLECTORS=(
    ["yield_curve_collector.py"]="📈 Кривая доходности казначейства США"
    ["real_yield_curve_collector.py"]="📊 Реальная кривая доходности"
    ["real_m2_collector.py"]="💵 Реальная денежная масса M2"
    ["building_permits_collector.py"]="🏗️ Разрешения на строительство"
    ["umcsi_collector.py"]="🛒 Потребительские настроения Michigan"
    ["ism_manufacturing_collector.py"]="🏭 ISM Manufacturing PMI"
    ["gdp_collector.py"]="🏛️ Реальный ВВП США"
)

# Активация виртуального окружения
activate_venv() {
    if [ ! -f "$VENV_DIR/bin/activate" ]; then
        print_error "Виртуальное окружение не найдено"
        exit 1
    fi
    source "$VENV_DIR/bin/activate"
    print_success "Виртуальное окружение активировано"
}

# Запуск одного коллектора
run_collector() {
    local script_name="$1"
    local description="$2"
    local script_path="$COLLECTORS_DIR/$script_name"
    
    if [ ! -f "$script_path" ]; then
        print_warning "Скрипт не найден: $script_name"
        return 1
    fi
    
    print_status "$description"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    local start_time=$(date +%s)
    
    if [ "$VERBOSE" = true ]; then
        python "$script_path"
    else
        python "$script_path" 2>/dev/null || {
            print_error "Ошибка в $script_name"
            return 1
        }
    fi
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    print_success "$description завершен за ${duration}с"
    echo ""
    
    return 0
}

# Проверка статуса базы данных
check_database_status() {
    print_status "🔍 Проверка состояния базы данных..."
    
    if [ -f "$PROJECT_DIR/_db_inspector.py" ]; then
        python "$PROJECT_DIR/_db_inspector.py" 2>/dev/null | head -20
    else
        print_warning "Скрипт проверки БД не найден"
    fi
    
    echo ""
}

# Быстрый режим (только критичные индикаторы)
run_quick_update() {
    local quick_collectors=(
        "gdp_collector.py"
        "umcsi_collector.py"
        "ism_manufacturing_collector.py"
    )
    
    print_header "БЫСТРОЕ ОБНОВЛЕНИЕ КЛЮЧЕВЫХ ИНДИКАТОРОВ"
    
    local success_count=0
    local total_count=${#quick_collectors[@]}
    
    for script in "${quick_collectors[@]}"; do
        if [ -n "${COLLECTORS[$script]}" ]; then
            if run_collector "$script" "${COLLECTORS[$script]}"; then
                ((success_count++))
            fi
        fi
    done
    
    echo ""
    print_status "Результат быстрого обновления: $success_count/$total_count успешно"
}

# Полное обновление всех индикаторов
run_full_update() {
    print_header "ПОЛНОЕ ОБНОВЛЕНИЕ ВСЕХ ИНДИКАТОРОВ"
    
    local success_count=0
    local total_count=${#COLLECTORS[@]}
    local failed_collectors=()
    
    for script in "${!COLLECTORS[@]}"; do
        if run_collector "$script" "${COLLECTORS[$script]}"; then
            ((success_count++))
        else
            failed_collectors+=("$script")
        fi
    done
    
    echo ""
    print_status "Результат полного обновления: $success_count/$total_count успешно"
    
    if [ ${#failed_collectors[@]} -gt 0 ]; then
        print_warning "Неудачные коллекторы:"
        for failed in "${failed_collectors[@]}"; do
            echo "  - $failed"
        done
    fi
}

# Показать помощь
show_help() {
    echo "Использование: $0 [опции]"
    echo ""
    echo "Опции:"
    echo "  --quick     Быстрое обновление (только ключевые индикаторы)"
    echo "  --verbose   Подробный вывод ошибок"
    echo "  --status    Показать только статус базы данных"
    echo "  --list      Показать список доступных коллекторов"
    echo "  --help      Показать эту справку"
    echo ""
    echo "Примеры:"
    echo "  $0              Полное обновление всех индикаторов"
    echo "  $0 --quick      Обновление только ключевых индикаторов"
    echo "  $0 --status     Проверка состояния базы данных"
}

# Показать список коллекторов
show_collectors() {
    print_header "ДОСТУПНЫЕ КОЛЛЕКТОРЫ"
    
    for script in "${!COLLECTORS[@]}"; do
        local script_path="$COLLECTORS_DIR/$script"
        local status="❌"
        if [ -f "$script_path" ]; then
            status="✅"
        fi
        echo -e "$status $script - ${COLLECTORS[$script]}"
    done
    echo ""
}

# Главная функция
main() {
    local start_time=$(date +%s)
    
    # Обработка аргументов
    while [[ $# -gt 0 ]]; do
        case $1 in
            --quick)
                QUICK_MODE=true
                shift
                ;;
            --verbose)
                VERBOSE=true
                shift
                ;;
            --status)
                activate_venv
                check_database_status
                exit 0
                ;;
            --list)
                show_collectors
                exit 0
                ;;
            --help|-h)
                show_help
                exit 0
                ;;
            *)
                print_error "Неизвестная опция: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    # Активация окружения
    activate_venv
    
    # Проверка состояния перед началом
    if [ "$QUICK_MODE" = false ]; then
        check_database_status
    fi
    
    # Запуск обновления
    if [ "$QUICK_MODE" = true ]; then
        run_quick_update
    else
        run_full_update
    fi
    
    # Проверка состояния после обновления
    if [ "$QUICK_MODE" = false ]; then
        echo ""
        check_database_status
    fi
    
    # Итоговая статистика
    local end_time=$(date +%s)
    local total_duration=$((end_time - start_time))
    
    print_header "ОБНОВЛЕНИЕ ЗАВЕРШЕНО"
    print_success "Общее время выполнения: ${total_duration}с"
    print_status "Время завершения: $(date '+%Y-%m-%d %H:%M:%S')"
}

# Обработка прерывания
trap 'print_warning "Операция прервана пользователем"; exit 130' INT TERM

# Запуск
main "$@"