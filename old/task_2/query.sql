--WAY 1
select count(player_id) as player_amount, event_date, game_id
into [#temp_table]
from user_logins
group by event_date, game_id;

select
    t_b.event_date,
    t_b.player_amount as user_logined,
    t_a.player_amount as users_logined_at_day_30,
    t_a.player_amount/t_b.player_amount as retention,
    t_b.game_id 
from [#temp_table] t_b
left join [#temp_table] as t_a
on t_b.game_id = t_a.game_id and t_b.event_date = date_add("day", t_a.event_date, -30)


--WAY 2
select distinct
    event_date,
    player_amount_before as user_logined,
    player_amount_after as users_logined_at_day_30,
    player_amount_after/player_amount_before as retention, 
    game_id
from (
    select
        t_b.event_date,
        count(t_b.player_id) over(partition by t_b.event_date, t_b.game_id) as player_amount_before,
        count(t_a.player_id) over(partition by t_a.event_date, t_a.game_id) as player_amount_after,
        t_b.game_id
    from user_logins as t_b
    left join user_logins as t_a
    on t_b.event_date = date_add("day", t_a.event_date, -30) and t_b.game_id = t_a.game_id) t;
