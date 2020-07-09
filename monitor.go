package main
//second copy
import (
	"bytes"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"flag"

	"fmt"

	"os"

	"github.com/Shopify/sarama"
	"github.com/olekukonko/tablewriter"
	"github.com/samuel/go-zookeeper/zk"
)

var (
	zkAddr            = flag.String("zkAddr", "", "zookeeper address")
	basePath          = flag.String("basePath", "/kafka/", "kafka base path in zookeeper")
	brokers           = flag.String("brokers", "10.254.20.21:9093, 10.254.20.22:9093, 10.254.20.23:9093", "brokers' address")
	topic             = flag.String("topic", "com.sap.dsc.ac.v1.template.int.commit-stage.d03abdcb-caf7-48bc-a35c-1c52b086f491", "topic name")
	group             = flag.String("group", "message-synthesizer-commit-stage", "consumer group name")
	version           = flag.String("version", "0.10.0.1", "kafka version. min version is 0.8.2.0")
	lagThreshold      = flag.Int("lagThreshold", 1, "alarm lag threshold for partition")
	totalLagThreshold = flag.Int("totalLagThreshold", 1, "alarm total lag threshold for topic")
	interval          = flag.Duration("duration", time.Minute, "check interval time")
	informEmail       = flag.String("email", "kunal.jaiswal.sap@gmail.com", "inform user email")
	smtpHost          = flag.String("mail.smtp.host", "smtp.gmail.com", "smtp host for sending alarms")
	smtpPort          = flag.Int("mail.smtp.port", 465, "smtp port for sending alarms")
	smtpUser          = flag.String("mail.smtp.user", "kunal.jaiswal.sap@gmail.com", "smtp user for sending alarms")
	smtpPassword      = flag.String("mail.smtp.password", "Zaq!2wsxcde3", "smtp user password for sending alarms")

)

var (
	maybeProblem       = false
	restored           = true
	lastTriggeredTime  time.Time
	mergeAlertDuration = 1 * time.Second
	kafkaVersions      = kafkaVersion()
)

func kafkaVersion() map[string]sarama.KafkaVersion {
	m := make(map[string]sarama.KafkaVersion)
	m["0.8.2.0"] = sarama.V0_8_2_0
	m["0.8.2.1"] = sarama.V0_8_2_1
	m["0.8.2.2"] = sarama.V0_8_2_2
	m["0.9.0.0"] = sarama.V0_9_0_0
	m["0.9.0.1"] = sarama.V0_9_0_1
	m["0.10.0.0"] = sarama.V0_10_0_0
	m["0.10.0.1"] = sarama.V0_10_0_1
	m["0.10.1.0"] = sarama.V0_10_1_0
	return m
}

func main() {
	flag.Parse()

	for {
		check()
	}
}

func check() {
	l := log.New(os.Stdout, "sample-srv ", log.LstdFlags|log.Lshortfile)
	l.Println("server started")
	kafkaBrokers := strings.Split(*brokers, ",")
	sort.Sort(sort.StringSlice(kafkaBrokers))

	v := kafkaVersions[*version]
	client := NewSaramaClient(kafkaBrokers, v)

	var buf bytes.Buffer
	var err error
	var c *zk.Conn

	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("check error: %v", r)

			bytes := buf.Bytes()
			os.Stdout.Write(bytes)

			client.Close()

			subject := fmt.Sprintf("Alarm: topic=%s, brokers: %s", *topic, *brokers)
			alert(*informEmail, subject, []byte(fmt.Sprintf("%v", r)), *smtpHost, *smtpPort, *smtpUser, *smtpPassword)
		}
	}()

	if *zkAddr != "" {
		c, _, err = zk.Connect(strings.Split(*zkAddr, ","), 30*time.Second)
		if err != nil {
			panic(err)
		}

		defer c.Close()
	}

	ticker := time.NewTicker(*interval)
	for range ticker.C {
		buf.Reset()

		//check brokers change event-- working
		newKafkaBrokers := runtimeKafkaBrokers(client)
		s1 := strings.Join(kafkaBrokers, ",")
		s2 := strings.Join(newKafkaBrokers, ",")
		if s1 != s2 {
			subject := fmt.Sprintf("Broker changed: topic=%s, brokers: %s", *topic, *brokers)
			alert(*informEmail, subject, []byte(fmt.Sprintf("prior brokers: %s \n current brokers: %s\n", s1, s2)), *smtpHost, *smtpPort, *smtpUser, *smtpPassword)

			if len(newKafkaBrokers) > 0 {
				kafkaBrokers = newKafkaBrokers
				*brokers = s2
			}
		}

		partitions, err := client.Partitions(*topic)
		if err != nil {
			fmt.Printf("failed to get partitions for topic=%s, err=%v\n", *topic, err)
			panic(err)
		}

		writablePartitions, err := client.WritablePartitions(*topic)
		if err != nil {
			fmt.Printf("failed to get writable partitions for topic=%s, err=%v\n", *topic, err)
			panic(err)
		}

		if len(partitions) != len(writablePartitions) {
			buf.WriteString("some partitions are not writable\n")
			buf.WriteString(fmt.Sprintf("all partitions: %v\n", partitions))
			buf.WriteString(fmt.Sprintf("writable partitions: %v\n", writablePartitions))
			//TODO print unwritable partitions
			maybeProblem = true
		}

		buf.WriteString(fmt.Sprintf("Time: %s\n", time.Now().Format("2006-01-02 15:04:05")))
		buf.WriteString(fmt.Sprintf("Brokers:%s\n", *brokers))
		buf.WriteString(fmt.Sprintf("Version: %s\n", *version))
		buf.WriteString(fmt.Sprintf("Topic: %s\n", *topic))
		buf.WriteString(fmt.Sprintf("Group: %s\n", *group))
		buf.WriteString(fmt.Sprintf("Partitions: %s\n", strconv.Itoa(len(partitions))))

		infos := GetPartitionInfo(client, *topic, partitions, c, *basePath)
		if len(infos) > 0 {
			table := tablewriter.NewWriter(&buf)
			table.SetHeader([]string{"partition", "leader address", "leader", "replicas", "isr"})
			for _, info := range infos {
				replicas := fmt.Sprintf("%v", info.Replicas)
				replicas = replicas[1 : len(replicas)-1]
				replicas = compareString(replicas)
				isr := compareString(info.Isr)

				table.Append([]string{strconv.Itoa(int(info.Partition)), info.LeaderAddress, strconv.Itoa(int(info.Leader)), "[" + replicas + "]", "[" + isr + "]"})

				if replicas != isr {
					maybeProblem = true
				}
			}

			table.SetAlignment(tablewriter.ALIGN_LEFT)
			table.Render()

			buf.WriteString("\n\n")
		}
		//lag calculation start here
		if offsets, err := FetchOffsets(client, *topic, *group); err == nil {

			table := tablewriter.NewWriter(&buf)
			table.SetHeader([]string{"partition", "end of log", "group offset", "lag"})

			var totalLag int64
			for _, info := range offsets {
				lag := info.PartitionOffset - info.GroupOffset
				if int(lag) > *lagThreshold {
					maybeProblem = true
				}

				totalLag += lag
				table.Append([]string{strconv.Itoa(int(info.Partition)), strconv.Itoa(int(info.PartitionOffset)),
					strconv.Itoa(int(info.GroupOffset)), convertLag(lag, *lagThreshold)})
			}

			if int(totalLag) > *totalLagThreshold {
				maybeProblem = true
			}

			table.SetFooter([]string{"", "", "Total", convertLag(totalLag, *totalLagThreshold)})
			table.SetAlignment(tablewriter.ALIGN_LEFT)
			table.SetFooterAlignment(tablewriter.ALIGN_LEFT)
			table.Render()
		}

		//output tables to stdout
		bytes := buf.Bytes()
		os.Stdout.Write(bytes)

		//first issue check or ignore check exceeds mergeAlertDuration
		if maybeProblem && time.Since(lastTriggeredTime) > mergeAlertDuration {
			restored = false
			lastTriggeredTime = time.Now()
			subject := fmt.Sprintf("Alarm: topic=%s, brokers: %s", *topic, *brokers)
			alert(*informEmail, subject, bytes, *smtpHost, *smtpPort, *smtpUser, *smtpPassword)
		}

		//fixed
		if !maybeProblem && !restored {
			subject := fmt.Sprintf("Fixed: topic=%s, brokers: %s", *topic, *brokers)
			alert(*informEmail, subject, bytes, *smtpHost, *smtpPort, *smtpUser, *smtpPassword)
			restored = true
			lastTriggeredTime = time.Unix(0, 0)
		}

		maybeProblem = false
	}
}

func compareString(s string) string {
	s1 := strings.Split(s, " ")
	sort.Sort(sort.StringSlice(s1))

	return strings.Join(s1, " ")
}
func convertLag(lag int64, threshold int) string {
	lagStr := strconv.Itoa(int(lag))
	if int(lag) > threshold {
		lagStr = lagStr
	}

	return lagStr
}

func runtimeKafkaBrokers(client sarama.Client) []string {
	brokers := client.Brokers()
	var fetchedBrokers []string
	for _, b := range brokers {
		fetchedBrokers = append(fetchedBrokers, b.Addr())
	}

	sort.Sort(sort.StringSlice(fetchedBrokers))
	return fetchedBrokers
}

func NewSaramaClient(brokers []string, version sarama.KafkaVersion) sarama.Client {
	config := sarama.NewConfig()
	config.Version = version
	config.Consumer.Return.Errors = true
	config.Metadata.RefreshFrequency = 1 * time.Minute
	config.Metadata.Retry.Max = 10
	config.Net.MaxOpenRequests = 10
	config.Net.DialTimeout = 10 * time.Second
	config.Net.ReadTimeout = 10 * time.Second
	config.Net.WriteTimeout = 10 * time.Second

	client, err := sarama.NewClient(brokers, config)

	if err != nil {
		panic("Failed to start client: " + err.Error())
	}

	return client
}
