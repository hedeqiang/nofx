// @Target(handleRegister)
package handleRegister

import (
	"testing"

	"nofx/test/harness"
)

// RegisterTest 嵌入 BaseTest，可按需重写 Before/After 钩子
type RegisterTest struct {
	harness.BaseTest
}

// Before 在每个用例执行前被调用，使用注入的 rt.Env 提供 DB/API 地址等
func (rt *RegisterTest) Before(t *testing.T) {
	// 先调用父类的 Before 来做统一的准备/清理（例如根据 CSV flag=c 清理数据）
	rt.BaseTest.Before(t)

	// 打印出测试 API 地址，便于调试
	if rt.Env != nil {
		t.Logf("TestEnv API URL: %s", rt.Env.URL())
	} else {
		t.Log("Warning: Env is nil in Before")
	}
}

// After 可选的清理/断言
func (rt *RegisterTest) After(t *testing.T) {
	// no-op for now
}

// @RunWith(case01)
func TestHandleRegister(t *testing.T) {
	rt := &RegisterTest{}
	harness.RunCase(t, rt)
}
