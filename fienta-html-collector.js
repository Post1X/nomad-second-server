const FIENTA_CODE = () => `
async function collectAndSendFientaHtml() {
  console.log('Начинаем сбор HTML со страницы Fienta...');

  try {
    // Проверяем, что мы на правильной странице
    const currentUrl = window.location.href;
    if (!currentUrl.includes('fienta.com')) {
      console.error('❌ Ошибка: Вы должны быть на странице fienta.com');
      return;
    }

    console.log('📍 Текущий URL:', currentUrl);

    // Ждем загрузки страницы
    await new Promise(resolve => setTimeout(resolve, 2000));

    // Функция для нажатия на кнопку "Load more"
    const clickLoadMore = async () => {
      const loadMoreBtn = document.querySelector('button#load-more-btn');
      if (!loadMoreBtn) {
        return false;
      }
      
      // Проверяем видимость кнопки
      const isVisible = loadMoreBtn.offsetParent !== null && 
                       !loadMoreBtn.disabled && 
                       loadMoreBtn.style.display !== 'none';
      
      if (isVisible) {
        console.log('🔄 Нажимаем на кнопку "Load more"...');
        loadMoreBtn.scrollIntoView({ behavior: 'smooth', block: 'center' });
        await new Promise(resolve => setTimeout(resolve, 500)); // Небольшая задержка перед кликом
        loadMoreBtn.click();
        await new Promise(resolve => setTimeout(resolve, 2000)); // Ждем загрузки
        return true;
      }
      return false;
    };

    // Нажимаем на кнопку пока она есть
    let clickCount = 0;
    let hasMore = true;
    while (hasMore) {
      hasMore = await clickLoadMore();
      if (hasMore) {
        clickCount++;
        console.log(\`✅ Нажатие #\${clickCount} выполнено, ждем загрузки...\`);
        await new Promise(resolve => setTimeout(resolve, 3000)); // Дополнительное ожидание
      }
    }

    console.log(\`✅ Всего нажатий на "Load more": \${clickCount}\`);
    console.log('⏳ Ждем финальной загрузки...');
    await new Promise(resolve => setTimeout(resolve, 2000));

    // Получаем div#events
    const eventsDiv = document.querySelector('div#events');
    if (!eventsDiv) {
      console.error('❌ Ошибка: div#events не найден на странице!');
      return;
    }

    console.log('✅ div#events найден');
    
    // Подсчитываем количество мероприятий
    const eventCards = eventsDiv.querySelectorAll('article.event-card');
    const eventsCount = eventCards.length;
    console.log(\`📊 Найдено мероприятий: \${eventsCount}\`);
    
    // Парсим события в JSON
    const parseEvents = () => {
      const events = [];
      eventCards.forEach((card) => {
        try {
          const linkEl = card.querySelector('a[href*="fienta.com"]');
          const href = linkEl ? (linkEl.getAttribute('href') || '').split('#')[0].trim() : '';
          
          const titleEl = card.querySelector('.event-card-title h2');
          const title = titleEl ? titleEl.textContent.trim() : '';
          
          const smallPs = card.querySelectorAll('.event-card-body p.small');
          const dateText = smallPs[0] ? smallPs[0].textContent.trim() : '';
          const venueText = smallPs[1] ? smallPs[1].textContent.trim() : '';
          
          if (href && title) {
            events.push({
              href,
              title,
              date: dateText,
              venue: venueText
            });
          }
        } catch (err) {
          console.error('Ошибка при парсинге карточки:', err);
        }
      });
      return events;
    };
    
    const eventsData = parseEvents();
    console.log(\`✅ Извлечено \${eventsData.length} событий\`);
    
    // Создаем JSON
    const jsonData = JSON.stringify(eventsData, null, 2);
    const jsonSize = (jsonData.length / 1024).toFixed(2);
    console.log(\`📦 Размер JSON: \${jsonSize} KB\`);
    
    // Получаем HTML содержимое (для обратной совместимости)
    const htmlContent = eventsDiv.outerHTML;
    const htmlSize = (htmlContent.length / 1024).toFixed(2);
    console.log(\`📦 Размер HTML: \${htmlSize} KB\`);

    // Функция для копирования в буфер обмена
    const copyToClipboard = async (text) => {
      try {
        // Сначала пробуем современный API (если доступен и документ в фокусе)
        if (navigator.clipboard && navigator.clipboard.writeText && document.hasFocus()) {
          try {
            await navigator.clipboard.writeText(text);
            return true;
          } catch (err) {
            // Если не получилось, используем fallback
            console.log('Clipboard API failed, using fallback method');
          }
        }
        
        // Надежный fallback метод с textarea (работает всегда)
        const textarea = document.createElement('textarea');
        textarea.value = text;
        textarea.style.position = 'fixed';
        textarea.style.left = '-999999px';
        textarea.style.top = '-999999px';
        textarea.style.opacity = '0';
        document.body.appendChild(textarea);
        textarea.focus();
        textarea.select();
        
        let success = false;
        try {
          success = document.execCommand('copy');
        } catch (err) {
          console.error('execCommand failed:', err);
        }
        
        document.body.removeChild(textarea);
        return success;
      } catch (err) {
        console.error('Ошибка при копировании:', err);
        return false;
      }
    };

    // Сохраняем данные в глобальные переменные
    window.collectedFientaHtml = htmlContent;
    window.collectedFientaJson = jsonData;
    
    // Показываем alert с предложением скопировать
    const alertMessage = \`📊 Найдено \${eventsCount} мероприятий.\\n\\nJSON: \${jsonSize} KB (оптимизировано)\\nHTML: \${htmlSize} KB\\n\\nНажмите OK, чтобы скопировать JSON в буфер обмена.\`;
    
    const userConfirmed = confirm(alertMessage);
    
    if (userConfirmed) {
      // Возвращаем фокус на window перед копированием
      window.focus();
      // Небольшая задержка для восстановления фокуса
      await new Promise(resolve => setTimeout(resolve, 100));
      
      const copied = await copyToClipboard(jsonData);
      
      if (copied) {
        alert('✅ JSON успешно скопирован в буфер обмена!');
        console.log('✅ JSON скопирован в буфер обмена');
        console.log('💾 HTML также доступен в window.collectedFientaHtml');
      } else {
        alert('❌ Не удалось скопировать в буфер обмена. JSON доступен в window.collectedFientaJson');
        console.error('❌ Не удалось скопировать в буфер обмена');
      }
    }

  } catch (error) {
    console.error('❌ Критическая ошибка:', error);
    if (error.message) {
      console.error('Сообщение:', error.message);
    }
    if (error.stack) {
      console.error('Stack:', error.stack);
    }
  }
}

collectAndSendFientaHtml();
`;
