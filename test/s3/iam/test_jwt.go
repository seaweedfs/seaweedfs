package main

import (
	"fmt"
	"time"
	"encoding/base64"
	"github.com/golang-jwt/jwt/v5"
)

func main() {
	now := time.Now()
	signingKeyB64 := "dGVzdC1zaWduaW5nLWtleS0zMi1jaGFyYWN0ZXJzLWxvbmc="
	signingKey, _ := base64.StdEncoding.DecodeString(signingKeyB64)
	
	sessionId := fmt.Sprintf("test-session-admin-user-TestAdminRole-%d", now.Unix())
	roleArn := "arn:seaweed:iam::role/TestAdminRole"
	sessionName := "test-session-admin-user"
	principalArn := fmt.Sprintf("arn:seaweed:sts::assumed-role/TestAdminRole/%s", sessionName)
	
	sessionClaims := jwt.MapClaims{
		"iss":        "seaweedfs-sts",
		"sub":        sessionId,
		"iat":        now.Unix(),
		"exp":        now.Add(time.Hour).Unix(),
		"nbf":        now.Unix(),
		"typ":        "session",
		"role":       roleArn,
		"snam":       sessionName,
		"principal":  principalArn,
		"assumed":    principalArn,
		"assumed_at": now.Format(time.RFC3339Nano),
		"ext_uid":    "admin-user",
		"idp":        "test-oidc",
		"max_dur":    int64(time.Hour.Seconds()),
		"sid":        sessionId,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, sessionClaims)
	tokenString, _ := token.SignedString(signingKey)
	fmt.Println(tokenString)
}
