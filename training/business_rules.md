# Бизнес-правила NL2SQL (Phase 2)

1. `revenue_local` считается только по завершенным заказам (`status_order = 'done'`).
2. `cancelled_orders` считается только по отмененным заказам (`status_order = 'cancel'`).
3. `completed_trips` считается только по завершенным заказам (`status_order = 'done'`).
4. `declined_tenders` считается по тендерам со статусом `status_tender = 'decline'`.
5. `avg_order_value_local` означает средний чек по завершенным заказам (`status_order = 'done'`) и считается по `price_order_local`.
6. `avg_accept_time_seconds` считается как средняя разница между `driveraccept_timestamp` и `order_timestamp`; строки без `driveraccept_timestamp` не участвуют.
7. `avg_duration_seconds` и `avg_distance_meters` считаются только по успешным поездкам (`status_order = 'done'`).
8. Разрез `channel` в MVP соответствует полю `status_tender`, но для вопросов про отклонения водителями лучше использовать термин `tender_status`.
9. Разрез по дате выполняется через `order_timestamp` с дневной гранулярностью (`date_trunc('day', order_timestamp)`).
10. Разрез по часу заказа выполняется через `EXTRACT(HOUR FROM order_timestamp)`.
11. Для сравнения стартовой, тендерной и финальной цены используйте `price_start_local`, `price_tender_local` и `price_order_local` только по успешным заказам.
12. В аналитических ответах должны использоваться только read-only SQL-запросы (`SELECT`).
