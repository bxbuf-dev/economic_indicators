#!/bin/bash

# setup_cron.sh - Настройка автоматического обновления данных через cron
# Использование: ./setup_cron.sh [--remove]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"

# Цвета
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Создание лог-директории
create_log_dir() {
    local log_dir="$PROJECT_DIR/logs"
    if [ ! -d "$log_dir" ]; then
        mkdir -p "$log_dir"
        print_success "Создана директория для логов: $log_dir"
    fi
}

# Создание wrapper-скрипта для cron
create_cron_wrapper() {
    local wrapper_script="$PROJECT_DIR/cron_update_wrapper.sh"
    
    cat > "$wrapper_script" << 'EOL'
#!/bin/bash

# Wrapper скрипт для cron - обновление экономических индикаторов
# Создается автоматически через setup_cron.sh

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$PROJECT_DIR/logs"
DATE=$(date '+%Y%m%d_%H%M%S')
LOG_FILE="$LOG_DIR/cron_update_$DATE.log"

# Функция логирования
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Основная функция
main() {
    log "=== НАЧАЛО АВТОМАТИЧЕСКОГО ОБНОВЛЕНИЯ ==="
    log "Лог файл: $LOG_FILE"
    
    cd "$PROJECT_DIR"
    
    # Быстрое обновление ключевых индикаторов
    if ./update_all_indicators.sh --quick >> "$LOG_FILE" 2>&1; then
        log "✅ Обновление завершено успешно"
        
        # Очистка старых логов (старше 30 дней)
        find "$LOG_DIR" -name "cron_update_*.log" -mtime +30 -delete 2>/dev/null || true
        
    else
        log "❌ Ошибка при обновлении"
        exit 1
    fi
    
    log "=== КОНЕЦ АВТОМАТИЧЕСКОГО ОБНОВЛЕНИЯ ==="
}

main "$@"
EOL

    chmod +x "$wrapper_script"
    print_success "Создан wrapper скрипт: $wrapper_script"
}

# Добавление cron задач
setup_cron_jobs() {
    local wrapper_script="$PROJECT_DIR/cron_update_wrapper.sh"
    
    # Получаем текущий crontab
    local current_cron=$(crontab -l 2>/dev/null || echo "")
    
    # Проверяем, не добавлены ли уже наши задачи
    if echo "$current_cron" | grep -q "economic_indicators"; then
        print_warning "Cron задачи уже настроены"
        return 0
    fi
    
    # Создаем новый crontab с нашими задачами
    {
        echo "$current_cron"
        echo ""
        echo "# Economic Indicators Auto-Update"
        echo "# Обновление в рабочие дни в 8:00 и 18:00"
        echo "0 8 * * 1-5 $wrapper_script"
        echo "0 18 * * 1-5 $wrapper_script"
        echo ""
        echo "# Полное обновление в воскресенье в 9:00"
        echo "0 9 * * 0 $PROJECT_DIR/update_all_indicators.sh >> $PROJECT_DIR/logs/weekly_full_update.log 2>&1"
    } | crontab -
    
    print_success "Cron задачи настроены:"
    echo "  - Быстрое обновление: пн-пт в 8:00 и 18:00"
    echo "  - Полное обновление: воскресенье в 9:00"
}

# Удаление cron задач
remove_cron_jobs() {
    local current_cron=$(crontab -l 2>/dev/null || echo "")
    
    if ! echo "$current_cron" | grep -q "economic_indicators"; then
        print_warning "Cron задачи не найдены"
        return 0
    fi
    
    # Удаляем наши задачи из crontab
    echo "$current_cron" | grep -v "economic_indicators\|cron_update_wrapper\|update_all_indicators" | crontab -
    
    # Удаляем wrapper скрипт
    local wrapper_script="$PROJECT_DIR/cron_update_wrapper.sh"
    if [ -f "$wrapper_script" ]; then
        rm "$wrapper_script"
        print_success "Удален wrapper скрипт"
    fi
    
    print_success "Cron задачи удалены"
}

# Показать текущие cron задачи
show_cron_status() {
    echo "Текущие cron задачи для экономических индикаторов:"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    local current_cron=$(crontab -l 2>/dev/null || echo "")
    if echo "$current_cron" | grep -q "economic_indicators"; then
        echo "$current_cron" | grep -A 10 "economic_indicators"
    else
        print_warning "Cron задачи не настроены"
    fi
    
    echo ""
    echo "Логи обновлений:"
    local log_dir="$PROJECT_DIR/logs"
    if [ -d "$log_dir" ]; then
        ls -la "$log_dir"/*.log 2>/dev/null | tail -5 || echo "Логов пока нет"
    else
        echo "Директория логов не создана"
    fi
}

# Показать помощь
show_help() {
    echo "Настройка автоматического обновления экономических индикаторов"
    echo ""
    echo "Использование: $0 [опция]"
    echo ""
    echo "Опции:"
    echo "  --remove    Удалить cron задачи"
    echo "  --status    Показать текущие настройки"
    echo "  --help      Показать эту справку"
    echo ""
    echo "Без опций: настроить автоматическое обновление"
    echo ""
    echo "Расписание по умолчанию:"
    echo "  - Быстрое обновление: пн-пт в 8:00 и 18:00"
    echo "  - Полное обновление: воскресенье в 9:00"
}

# Главная функция
main() {
    case "${1:-}" in
        --remove)
            remove_cron_jobs
            ;;
        --status)
            show_cron_status
            ;;
        --help|-h)
            show_help
            ;;
        "")
            echo "Настройка автоматического обновления экономических индикаторов..."
            create_log_dir
            create_cron_wrapper
            setup_cron_jobs
            echo ""
            show_cron_status
            ;;
        *)
            print_error "Неизвестная опция: $1"
            show_help
            exit 1
            ;;
    esac
}

main "$@"