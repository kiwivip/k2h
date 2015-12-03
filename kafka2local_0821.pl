#!/bin/env perl

use 5.10.1;
use Redis ;
use Kafka::Connection;
use Kafka::Consumer;
use Kafka qw(
    $DEFAULT_MAX_BYTES
    $DEFAULT_MAX_NUMBER_OF_OFFSETS
    $RECEIVE_EARLIEST_OFFSETS
);
use Scalar::Util qw(
    blessed
);
use Try::Tiny;
 
use Kafka qw(
    $BITS64
);
use Kafka::Connection;
use Kafka::Producer;
use Kafka::Consumer ;
# ------------------------------------------------------------------------------------------

# ----------------------------------------------------------
# topic: bed_nginx_log l99_nginx_log designer_nginx_log
# ----------------------------------------------------------
my $topic_name = shift ;
die "Please add param - logName : l99_nginx_log / designer_nginx_log / bed_nginx_log ?" if $topic_name !~ /nginx_log/;

my $DIR = '/home/logs/' ;
my %num2month = (
	'01' => "Jan" , '02' => "Feb" , '03' => "Mar" , '04' => "Apr" ,
	'05' => "May" , '06' => "Jun" , '07' => "Jul" , '08' => "Aug" ,
	'09' => "Sep" , '10' => "Oct" , '11' => "Nov" , '12' => "Dec"
);
my %month2num = reverse %num2month ;

my $redis = Redis->new();

#say 'This is Kafka package ', $Kafka::VERSION;
#say 'You have a ', $BITS64 ? '64' : '32', ' bit system';
my $connection = Kafka::Connection->new( host => '192.168.201.57' ); 

#-- Producer
    
#-- Consumer
my $consumer = Kafka::Consumer->new( Connection  => $connection );

try
{
    while (1)
    {
        
   	my $rediskey_offset = $topic_name.'::offset' ; 
        my $offset = $redis->get($rediskey_offset);
        my $messages = $consumer -> fetch($topic_name , 0 , $offset);
    
        foreach my $message ( @$messages )
        {
            if( $message->valid )
            {
                my $offset = $message->offset ;
                my $mess = $message->payload ;
                
                # 按小时 拆开存储，方便hive直接load
                my ($year,$month,$day,$hour,$time_mm) ;
                if ($mess =~ /\s+\[(\d+)\/([a-zA-Z]+)\/(\d+):(\d+):(\d+:\d+) \S+]/)
                {
                    ($year,$day,$hour,$time_mm) = ($3,$1,$4,$5) ;
                    foreach($day,$hour){ $_ = sprintf("%02d", $_); }
                    my $month_s = $2 ;
                    $month = month2num($month_s) ;
                }
            
                my $dir_log = $DIR . $topic_name . '/' ;
                myDir($dir_log) ;
                open my $fh_log , ">>" , $dir_log.$year.$month.$day.$hour.'.raw' ;
                print $fh_log $mess."\n" ;
                
                # 在redis中存储最新的时段日志，给load进hive时用的
                $redis->incr($rediskey_offset) ;
                my $lasttime = "$year-$month-$day-$hour" ;
                my $rediskey_lasttime = $topic_name.'::lasttime' ;
                $redis -> set($rediskey_lasttime , $lasttime) ;
                
            }
            else {
                say 'error      : ', $message->error;
            }
        }
    }

}
catch
{
    if ( blessed( $_ ) && $_->isa( 'Kafka::Exception' ) ) {
        warn 'Error: (', $_->code, ') ',  $_->message, "\n";
        exit;
    } else {
        die $_;
    }
};
 
# cleaning up
undef $consumer;
#undef $producer;
undef $connection;

# ======================================== function =====================================

sub month2num
{
        return $month2num { $_[0] } ;
}

sub num2month
{
        return $num2month { $_[0] } ;
}

sub myDir
{
        my $dir = shift;
        my $mod = shift || '777';
        if ( ! -e $dir )
        {
                system "mkdir -p -m $mod $dir";
        }
}

=pod    
    my $producer = Kafka::Producer->new( Connection => $connection );
    $producer->send('cs',0,
    qq{192.168.199.57 api.nyx.l99.com - [5/Nov/2015:07:28:57 +0800] "GET /guide/type?machine_code=865647021279049&client=key%3ABedForAndroid-chuangshang_huawei&version=1.0&format=json HTTP/1.0" 200 897 "-" "com.l99.bed/4.1.0(Android OS 4.4.2,Che2-TL00)" "-" https 223.104.1.17 0.003 0.002 .}
    );

    my $offsets = $consumer->offsets
    (
        $topic_name,                    # topic
        0,                              # partition
        $RECEIVE_EARLIEST_OFFSETS,      # time
        $DEFAULT_MAX_NUMBER_OF_OFFSETS  # max_number
    );
 
    if( @$offsets ) {
        say "Received offset: $_" foreach @$offsets;
    } else {
        warn "Error: Offsets are not received\n";
    }  
=cut