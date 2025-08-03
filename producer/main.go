package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

var messages = []string{
	"govno", "Ебать дерьмо", "fortnite", "HAHAHAHHAHA", "BALLS🧴",
	"ты че ебанутый", "i'm gay", "чат говно", "I like boys", "legacy", "I kidnap autistic kids",
	"Chickawagga", "skibidi sigma toilet", "amongus sussy", "FAT ASS NIGGA", "GAY ASS NIGA",
	"your mom is in my DMs", "cringe overload", "whopper whopper", "touch grass", "fat nigga balls",
	"I know your 𝑔𝑎𝑦~", "Bye🤫🧏🏻‍♂️Bye🗿", "i hate niggers", "i eat semen", "nigga?",
	"🗺 in my ᶠᶸᶜᵏме𓀐𓂸", "╾━╤デ╦︻", "𝕊𝕌𝕊𝕊𝕐 𝔹𝔸𝕂𝔸", "google is watching", "i farted in VR",
	"i bark a™™t mailmen", "fat niga ball", "я спалил филимоньку за дрочкой", "bro what", "филимонька ебаный дрочила",
	"i vibe with pain", "i swallowed my cum", "goofy ahh ass", "my dog nigaa", "poop.exe govno",
	"discord kittens rise", "𝕾𝖐𝖎𝖇𝖎𝖉𝖎 𝕿𝖔𝖎𝖑𝖊𝖙", "i lost to a bot", "🆘help me", "gigachad energy",
	"tiktok likee", "bro is in a anal simulation", "stop breathing my ass", "sigma toilet man", "𝖘𝖍𝖚𝖙 𝖚𝖕 𝖋𝖆𝖌𝖌𝖔𝖙",
	"error: im retard", "my GPU is sussy🏳️‍🌈🏳️‍🌈", "i lick toes", "i run cos im niga", "hotdog is for fat nigas",
	"🏳‍🌈say🥛gex🥵", "yo 💎 so ⚔️", "woke up and chose violence", "alpha CICCA", "sh💀il h1tlerd",
	"i hacked my fridge", "math is a scam", "i identify as a router", "mcdonalds wifi certified", "my fish knows python",
	"roblox balls", "cursed chrome tabs", "i got banned from gay club", "shrek is love", "my cat is a mod",
	"NFTs gave me rabies", "zuckerberg stole my soul", "clippy haunts my 8=✊=D💦", "ඞyou are gඞy", "who touched my spaghet",
	"data leaked from my brain", "microwave gang", "keyboard warrior elite", "free robux not a scam", "i breathe memes",
	"🙏🏽🙏🏽🙏🏽", "windows update ruined my marriage", "npc moment", "mario ate my sandwich", "i simp for RAM",
	"bro is lagging IRL🧴", "i mine crypt🚔o with🚓 a hairdryer", "ඞ ඞ ඞ ඞ ඞ ඞ ඞ", "Skibidisigma🐺🥶", "reboot me daddy",
	"𝐍ιووεr 🙆🏾👨🏿‍🦱", "dropped my brain", "vibing in low resolution", "my wifi is allergic to rain", "catgirls are real",
	"based and keyboard-pilled", "voice cracked in 4K", "i bark at strangers", "streaming my breakdown", "mom said it’s my turn on the braincell",
	"thug.exe", "npcalert", "📞who asked", "sigmashake", "OhioExit", "🃜🃚🃖🃁🂭🂺", "rizzster", "WalterDrip", "Skibidini", "🛩🏢🏢 9/11",
	"ratio.exe", "swaglag", "sleepkilla", "chatlagged", "cringeunit", "susmode", "ghostkeyboard", "aimingirl", "botboy", "WiFiAlpha",
	"sigmabot9000", "𒅒𒈔𒅒𒇫𒄆", "gyattwave", "clippyrevenge", "npcwalk", "rizzler", "toiletwarrior", "ramlover", "griddybug",
	"ก็็็็็็็็็็็็็็็็็็็", "trashmind", "deltachat", "memeghost", "glitchboy", "npcdad", "gyatthunter", "yoinkcore", "nahfrfr", "shadowdrop",
	"drake😍𓀐𓂸𓀐𓂸", "𝙍𝘼𝙋𝙀 𝙒𝙊𝙈𝙀𝙉❤️‍🔥", "🧴🧴P DIDDY", "𝖘𝖍𝖚𝖙 𝖚𝖕 𝖋𝖆𝖌𝖌𝖔𝖙", "toiletspawner", "vibe.exe", "Adolf $hitler", "bludrisks", "꧁𝔂𝓪𝓼𝓼 𝓺𝓾𝓮𝓮𝓷꧂",
}

var nicknames = []string{
	"CoolGuy123", "LigmaBalls", "UwU_master", "SussyBaka", "GigaChad",
	"CringeLord", "NoobDestroyer", "ShadowStep", "ZoomerZed", "AI_Kisser",
	"beriven", "Lolly", "Nigaa", "bakka", "dr0nched",
	"dolbaeb", "nigawhat", "shadowfriend", "iRis.EXE", "WaulterWhite",
	"killer12231", "121451512", "hipy", "litlitlit", "WIRE",
	"uaid", "defanlan", "gooningLord", "feetlover", "analo",
}

func generateGilup() string {
	msg := messages[rand.Intn(len(messages))]
	nick := nicknames[rand.Intn(len(nicknames))]

	return fmt.Sprintf("%s: %s", nick, msg)
}

func sendMessage(wg *sync.WaitGroup) {
	defer wg.Done()

	ctx := context.Background()

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        "porn",
		RequiredAcks: 0,
		BatchSize:    1,
		BatchTimeout: 0,
	})
	defer writer.Close()

	for range 6 {
		finalMsg := generateGilup()

		err := writer.WriteMessages(ctx, kafka.Message{
			Value: []byte(finalMsg),
		})
		if err != nil {
			log.Println("Ошибка при отправке:", err)
			return
		}
	}
}

func main() {
	const parallel = 100
	var wg sync.WaitGroup
	wg.Add(parallel)

	for range parallel {
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		go sendMessage(&wg)
	}

	wg.Wait()
}
