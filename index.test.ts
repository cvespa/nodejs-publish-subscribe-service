// eslint-disable-next-line import/no-named-as-default
import Service, { Service as _Service } from "./index";

describe("Index", () => {
  test("should have proper elements", () => {
    expect(Service).toBe(_Service);
  });
});
