<?
W::add_mixin('DbMixin');

foreach($config['databases'] as $name=>$settings)
{
  W::db_add($name, $settings);
}
W::db_select('default');
