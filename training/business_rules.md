# Бизнес-правила NL2SQL (Phase 2)

1. `revenue_local` считается только по завершенным заказам (`status_order = 'done'`).
2. `cancelled_orders` считается только по отмененным заказам (`status_order = 'cancel'`).
3. `completed_trips` считается только по завершенным заказам (`status_order = 'done'`).
4. Разрез `channel` в MVP соответствует полю `status_tender`.
5. Разрез по дате выполняется через `order_timestamp` с дневной гранулярностью (`date_trunc('day', order_timestamp)`).
6. В аналитических ответах должны использоваться только read-only SQL-запросы (`SELECT`).
