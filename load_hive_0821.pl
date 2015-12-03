#!/bin/env perl

use 5.10.1;
use Redis ;
use POSIX qw(strftime);

my @topic_names = (
		    'bed_nginx_log'  ,
		    'designer_nginx_log' ,
		    'l99_nginx_log' 
		  ) ;
my $DIR = '/home/logs/' ;
my $hour_step = 2 ;						# 本次运行你想往前扫描多少个小时?

my $redis = Redis->new();


foreach (@topic_names)
{
    my $topic_name = $_ ;
    my $lasttime = $redis -> get($topic_name.'::lasttime') ;

    for ( 1 .. $hour_step)
    {
        my $hour_step = $_ ;
        my $time_target = strftime("%Y-%m-%d-%H", localtime(time() - 3600 * $hour_step)) ;
        my ($year,$month,$day,$hour) = $time_target =~ /^(\d+)-(\d+)-(\d+)-(\d+)/ ;
        
        next if $time_target gt $lasttime ;				# 还没有从kafka上拉下来的跳过
        next if $time_target eq $lasttime ;				# 正在写入的该小时日志跳过
        if ($redis -> get($topic_name.'::load_'."$year-$month-$day-$hour") ){
            say "$topic_name : $year-$month-$day-$hour loaded ." ;
            next ;							# 已经load进hive的跳过
        }
    
        my $log_raw   = $DIR . $topic_name . '/' .$year.$month.$day.$hour.'.raw' ;
        next unless -e $log_raw ;
        my $log_parse = $DIR . $topic_name . '/' .$year.$month.$day.$hour.'.new' ;
        #say $log_raw ;
        say $log_parse ;
	#=pod
        open my $fh_new , ">>" , $log_parse ;
        open my $fh_log , "<"  , $log_raw ;
        while (<$fh_log>)
        {
            # ---------------------------------------------------------------------------------------------------------
            # bed_nginx_log 
	    # 192.168.199.57 api.nyx.l99.com - [5/Nov/2015:07:28:57 +0800] "GET /guide/type?machine_code=865647021279049&client=key%3ABedForAndroid-chuangshang_huawei&version=1.0&format=json HTTP/1.0" 200 897 "-" "com.l99.bed/4.1.0(Android OS 4.4.2,Che2-TL00)" "-" https 223.104.1.17 0.003 0.002 .
            # 192.168.199.57 api.nyx.l99.com iztIQV8k/SxouspvRtANdQ== [19/May/2015:12:00:01 +0800]
	    #   "GET /space/info/view?target_id=4875942 HTTP/1.1"
	    #     200 352 "-" "Java/1.6.0_25" "-" http 223.203.222.94 0.029 0.028 .
	    #
	    # --------------------------------------------------------------------------------------------------------
	    # designer_nginx_log 
	    # 192.168.199.57 api.designer.l99.com 13260758 [27/Jul/2015:01:08:02 +0800]
	    #   "GET /designer/media/content?format=json&client=key%3ADesignerForiPhone&limit=19&language=zh_CN&version=1.33&lat=0.000000&lng=0.000000&machine_code=5F62490B-46D2-4BF1-A4B5-F1D6BE71AE84 HTTP/1.1"
	    #     200 8439 "-" "com.l99.designer/1.33 (5612, iPhone OS 8.3, iPhone7,1, Scale/3.0)" "4.89" http 121.32.149.212 0.159 0.159 .
	    #
	    # -------------------------------------------------------------------------------------------------------
	    # l99_nginx_log
	    # 192.168.199.57 www.l99.com - [27/Jul/2015:01:59:59 +0800]
	    #   "GET /EditText_view.action?textId=581524 HTTP/1.1"
	    #     200 24594 "http://m.baidu.com/s?word=%E5%90%8C%E6%A1%8C%E5%9C%A8%E5%AE%B6%E5%B9%B2%E4%BA%86%E6%88%91&from=1001560k&pu=ofrom%401001560k&sa=opr_1_1"
	    #     "Mozilla/5.0 (Linux; Android 5.0.2; X600 Build/ABXCNOP5000306151S) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/37.0.0.0 Mobile Safari/537.36" "2.35" http 223.104.27.14 0.065 0.065 .
	    # --------------------------------------------------------------------------------------------------------
	    chomp ;
	    if (/^(\S+) (\S+) (\S+) \[\d+\/[a-zA-Z]+\/\d+:\d+:(\d+:\d+) \S+]\s+"([\s\S]*?)" (\S+) \S+ "\S+" "([\s\S]+)" "\S+" \S+ ([\.0-9]+) \S+ \S+ \.$/) {
	        my ($ip_1,$api_name,$l99NO,$time_r,$get,$status,$agent,$ip_user) = ($1,$2,$3,$4,$5,$6,$7,$8) ;
	        my $time = "$year-$month-$day $hour:$time_r" ;
	        my $log_p = join "\t" , ($ip_1,$api_name,$l99NO,$time,$get,$status,$agent,$ip_user) ;
	        print $fh_new $log_p."\n" ;
	        
	    }
	    
	}
	my $cmd = qq{/home/hive/bin/hive -e 'load data local inpath "$log_parse" into table $topic_name partition (year='$year',month="$month",day="$day",hour="$hour")';} ;
	#say $cmd ;
	my $sig = system("$cmd") ;
	if ($sig == 0) {
	    say "$topic_name : $year-$month-$day-$hour load success .\n";
	    $redis -> set($topic_name.'::load_'."$year-$month-$day-$hour" , 1) ;
	    system("gzip $log_raw") ;
	    system("rm -f $log_parse") ;
	}
    }
}

