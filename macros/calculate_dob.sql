-- may expand on the function sometime to calculate weeks etc. 
-- But for now it calculates number of months and number of years between date parts

create or replace function {{target.schema}}.datediffc(interval string, start_date date, end_date date)
returns integer
as 
$$
case 
  when interval = 'years'
  then
    case 
      when dateadd(year, datediff(years, start_date, end_date), start_date) > end_date
      then datediff(years, start_date, end_date) - 1
      else datediff(years, start_date, end_date)
    end
  when interval = 'months'
  then
    case
      when dateadd(month, datediff(months, start_date, end_date), start_date) > end_date
      then datediff(months, start_date, end_date) - 1
      else datediff(months, start_date, end_date)
    end
  when interval = 'days'
  then datediff(days, start_date, end_date)
end
$$
